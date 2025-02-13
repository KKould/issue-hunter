use anyhow::anyhow;
use chrono::{DateTime, Datelike, NaiveDateTime, TimeZone, Utc};
use clap::{Parser, Subcommand};
use kite_sql::db::{DataBaseBuilder, Database, ResultIter};
use kite_sql::implement_from_tuple;
use kite_sql::storage::rocksdb::RocksStorage;
use kite_sql::types::value::DataValue;
use prettytable::{row, Table};
use serde::Deserialize;
use std::cmp::min;
use std::fmt::Write;

type SqlBase = Database<RocksStorage>;

#[derive(Deserialize, Debug, Default)]
struct Issue {
    id: u64,
    number: u64,
    title: String,
    state: String,
    #[serde(skip)]
    repo_name: String,
    #[serde(skip)]
    user_id: u64,
    user: User,
    labels: Vec<Label>,
    created_at: DateTime<Utc>,
}

implement_from_tuple!(
    Issue, (
        id: u64 => |inner: &mut Issue, value: DataValue| {
            inner.id = value.u64().unwrap();
        },
        number: u64 => |inner: &mut Issue, value: DataValue| {
            inner.number = value.u64().unwrap();
        },
        title: String => |inner: &mut Issue, value: DataValue| {
            inner.title = value.utf8().unwrap().to_string();
        },
        state: String => |inner: &mut Issue, value: DataValue| {
            inner.state = value.utf8().unwrap().to_string();
        },
        repo_name: String => |inner: &mut Issue, value: DataValue| {
            inner.repo_name = value.utf8().unwrap().to_string();
        },
        user_id: u64 => |inner: &mut Issue, value: DataValue| {
            inner.user_id = value.u64().unwrap();
        },
        created_at: NaiveDateTime => |inner: &mut Issue, value: DataValue| {
            inner.created_at = value.datetime().unwrap().and_utc();
        }
    )
);

struct IssueLabelLink {
    issue_id: u64,
    label_id: u64,
}

#[derive(Deserialize, Debug, Default)]
struct User {
    id: u64,
    login: String,
}

implement_from_tuple!(
    User, (
        id: u64 => |inner: &mut User, value: DataValue| {
            inner.id = value.u64().unwrap();
        },
        login: String => |inner: &mut User, value: DataValue| {
            inner.login = value.utf8().unwrap().to_string();
        }
    )
);

#[derive(Deserialize, Hash, Debug, Default, PartialEq, Eq)]
struct Label {
    id: u64,
    name: String,
    description: Option<String>,
}

implement_from_tuple!(
    Label, (
        id: u64 => |inner: &mut Label, value: DataValue| {
            inner.id = value.u64().unwrap();
        },
        name: String => |inner: &mut Label, value: DataValue| {
            inner.name = value.utf8().unwrap().to_string();
        },
        description: String => |inner: &mut Label, value: DataValue| {
            inner.description = value.utf8().map(|s| s.to_string());
        }
    )
);

#[derive(Deserialize, Parser, Debug, Default)]
struct Repo {
    #[clap(name = "owner", long)]
    owner_name: String,
    #[clap(long)]
    name: String,
}

impl Repo {
    fn full_name(&self) -> String {
        format!("{}/{}", self.owner_name, self.name)
    }
}

implement_from_tuple!(
    Repo, (
        owner_name: String => |inner: &mut Repo, value: DataValue| {
            inner.owner_name = value.utf8().unwrap().to_string();
        },
        name: String => |inner: &mut Repo, value: DataValue| {
            inner.name = value.utf8().unwrap().to_string();
        }
    )
);

trait Bean {
    fn insert(&self, database: &SqlBase) -> anyhow::Result<()>;
    fn delete(&self, database: &SqlBase) -> anyhow::Result<()>;
}

impl Bean for Label {
    fn insert(&self, database: &SqlBase) -> anyhow::Result<()> {
        database
            .run(format!(
                "insert overwrite labels values({}, '{}', '{}');",
                self.id,
                escape_sql_string(&self.name),
                self.description
                    .as_ref()
                    .map(|s| escape_sql_string(s))
                    .unwrap_or("null".to_string()),
            ))?
            .done()?;

        Ok(())
    }

    fn delete(&self, database: &SqlBase) -> anyhow::Result<()> {
        database
            .run(format!("delete from labels where id = {};", self.id))?
            .done()?;

        Ok(())
    }
}

impl Bean for IssueLabelLink {
    fn insert(&self, database: &SqlBase) -> anyhow::Result<()> {
        database
            .run(format!(
                "insert overwrite issue_labels values({}, {});",
                self.issue_id, self.label_id
            ))?
            .done()?;

        Ok(())
    }

    fn delete(&self, database: &SqlBase) -> anyhow::Result<()> {
        database
            .run(format!(
                "delete from issue_labels where issue_id = {} and label_id = {};",
                self.issue_id, self.label_id
            ))?
            .done()?;

        Ok(())
    }
}

impl Bean for Repo {
    fn insert(&self, database: &SqlBase) -> anyhow::Result<()> {
        database
            .run(format!(
                "insert overwrite repos values('{}', '{}');",
                self.owner_name, self.name
            ))?
            .done()?;

        Ok(())
    }

    fn delete(&self, database: &SqlBase) -> anyhow::Result<()> {
        database
            .run(format!(
                "delete from repos where owner_name = '{}' and name = '{}';",
                self.owner_name, self.name
            ))?
            .done()?;
        database
            .run(format!(
                "delete from issues where repo_name = '{}';",
                self.full_name()
            ))?
            .done()?;

        Ok(())
    }
}

impl Bean for User {
    fn insert(&self, database: &SqlBase) -> anyhow::Result<()> {
        database
            .run(format!(
                "insert overwrite users values({}, '{}');",
                self.id, self.login
            ))?
            .done()?;

        Ok(())
    }

    fn delete(&self, database: &SqlBase) -> anyhow::Result<()> {
        database
            .run(format!("delete from users where id = {};", self.id))?
            .done()?;

        Ok(())
    }
}

impl Bean for Issue {
    fn insert(&self, database: &SqlBase) -> anyhow::Result<()> {
        database
            .run(format!(
                "insert overwrite issues values({}, {}, '{}', '{}', '{}', {}, '{}');",
                self.id,
                self.number,
                escape_sql_string(&self.title),
                self.state,
                self.repo_name,
                self.user.id,
                self.created_at.format("%Y-%m-%d %H:%M:%S"),
            ))?
            .done()?;
        self.user.insert(database)?;
        for label in &self.labels {
            IssueLabelLink {
                issue_id: self.id,
                label_id: label.id,
            }
            .insert(database)?;
            label.insert(database)?;
        }

        Ok(())
    }

    fn delete(&self, database: &SqlBase) -> anyhow::Result<()> {
        database
            .run(format!("delete from issues where id = {};", self.id))?
            .done()?;
        database
            .run(format!(
                "delete from issue_labels where issue_id = {};",
                self.id
            ))?
            .done()?;

        Ok(())
    }
}

impl Issue {
    fn load_user(&mut self, database: &SqlBase) -> anyhow::Result<()> {
        let mut iter = database.run(format!("select * from users where id = {}", self.user_id))?;
        let schema = iter.schema().clone();
        let tuple = iter
            .next()
            .transpose()?
            .unwrap_or_else(|| panic!("user: {} not found", self.user_id));
        self.user = User::from((&schema, tuple));

        Ok(())
    }

    fn load_labels(&mut self, database: &SqlBase) -> anyhow::Result<()> {
        let iter = database.run(format!("SELECT l.* FROM labels l INNER JOIN issue_labels il ON l.id = il.label_id WHERE il.issue_id = {};", self.id))?;
        let schema = iter.schema().clone();

        self.labels.clear();
        for tuple in iter {
            self.labels.push(Label::from((&schema, tuple?)));
        }

        Ok(())
    }
}

fn escape_sql_string(input: &str) -> String {
    input.replace("'", "''")
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    let dir_path = dirs::home_dir()
        .expect("Your system does not have a Config directory!")
        .join("issue-hunter");
    let database = DataBaseBuilder::path(dir_path).build()?;
    let client = Client {
        client: Default::default(),
        database,
    };
    client.create_table()?;

    match &cli.command {
        Command::Update(args) => {
            client.update_issues(args).await?;
        }
        Command::AddRepo(repo) => {
            client.add_repo(repo)?;
        }
        Command::RemoveRepo(repo) => {
            client.remove_repo(repo)?;
        }
        Command::Fetch(args) => {
            let mut table = Table::new();

            table.add_row(row![
                "ID",
                "Number",
                "Repository",
                "Title",
                "State",
                "User",
                "Labels",
                "Created At"
            ]);

            for issue in client.fetch_issues(args)? {
                let mut issue = issue?;
                issue.load_user(&client.database)?;
                issue.load_labels(&client.database)?;

                let labels = issue
                    .labels
                    .iter()
                    .map(|label| label.name.clone())
                    .collect::<Vec<_>>()
                    .join(", ");

                table.add_row(row![
                    issue.id,
                    issue.number,
                    issue.repo_name,
                    issue.title,
                    issue.state,
                    issue.user.login,
                    labels,
                    issue.created_at
                ]);
            }

            table.printstd();
        }
        Command::Repos => {
            let mut table = Table::new();

            table.add_row(row!["Owner", "Name", "Url"]);

            for repo in client.repos()? {
                let repo = repo?;

                table.add_row(row![
                    repo.owner_name,
                    repo.name,
                    format!("https://github.com/{}", repo.full_name())
                ]);
            }

            table.printstd();
        }
    }

    Ok(())
}

struct Client {
    client: reqwest::Client,
    database: SqlBase,
}

#[derive(Parser, Debug)]
#[clap(
    name = "issue-hunter",
    version = "0.0.1",
    author = "kould",
    about = "A tool for collecting and tracking issues from multiple repositories in one place."
)]
struct Cli {
    #[clap(subcommand)]
    command: Command,
}

#[derive(Subcommand, Debug)]
enum Command {
    Update(UpdateArgs),
    AddRepo(Repo),
    RemoveRepo(Repo),
    Fetch(FetchArgs),
    Repos,
}

#[derive(Parser, Debug)]
struct UpdateArgs {
    #[clap(long)]
    create_after: Option<DateTime<Utc>>,
}

#[derive(Parser, Debug)]
struct FetchArgs {
    #[clap(long)]
    repo_name: Option<String>,
    #[clap(long)]
    create_after: Option<DateTime<Utc>>,
    #[clap(long, action, default_value = "false")]
    today: bool,
    #[clap(long)]
    label_name: Option<String>,
    #[clap(long, default_value = "1")]
    page: usize,
    #[clap(long, default_value = "10")]
    page_num: usize,
}

impl Client {
    fn create_table(&self) -> anyhow::Result<()> {
        self.database
            .run(
                "CREATE TABLE IF NOT EXISTS repos (
    owner_name VARCHAR(255) NOT NULL,
    name VARCHAR(255) NOT NULL,
    PRIMARY KEY (owner_name, name)
);",
            )?
            .done()?;
        self.database
            .run(
                "CREATE TABLE IF NOT EXISTS users (
    id BIGINT PRIMARY KEY,
    login VARCHAR(255) NOT NULL
);",
            )?
            .done()?;
        self.database
            .run(
                "CREATE TABLE IF NOT EXISTS labels (
    id BIGINT PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    description VARCHAR(255)
);",
            )?
            .done()?;
        self.database
            .run(
                "CREATE TABLE IF NOT EXISTS issues (
    id BIGINT PRIMARY KEY,
    number BIGINT NOT NULL,
    title TEXT NOT NULL,
    state VARCHAR(50) NOT NULL,
    repo_name VARCHAR(255) NOT NULL,
    user_id BIGINT NOT NULL,
    created_at DATETIME NOT NULL
);",
            )?
            .done()?;
        self.database
            .run(
                "CREATE TABLE IF NOT EXISTS issue_labels (
    issue_id BIGINT,
    label_id BIGINT,
    PRIMARY KEY (issue_id, label_id)
);",
            )?
            .done()?;

        Ok(())
    }

    fn fetch_issues<'a>(
        &'a self,
        args: &FetchArgs,
    ) -> anyhow::Result<impl Iterator<Item = Result<Issue, anyhow::Error>> + 'a> {
        let mut query = "select * from issues where 1 = 1".to_string();

        if let Some(repo_name) = &args.repo_name {
            query.push_str(&format!(" and repo_name like '{}'", repo_name));
        }

        let mut create_after = args
            .create_after
            .map(|time| time.format("%Y-%m-%d %H:%M:%S").to_string());
        if args.today {
            create_after = Some(Utc::now().date_naive().format("%Y-%m-%d").to_string());
        }
        if let Some(create_after) = create_after {
            query.push_str(&format!(" and created_at > '{}'", create_after));
        }
        if let Some(label_name) = &args.label_name {
            let label_id = self
                .database
                .run(format!(
                    "select id from labels where name = '{}'",
                    label_name
                ))?
                .next()
                .transpose()?
                .unwrap_or_else(|| panic!("Label: '{}' not Found", label_name))
                .values[0]
                .u64()
                .unwrap();

            // TODO: Cache issue_ids;
            let mut issue_ids = Vec::new();
            for result in self.database.run(format!(
                "select issue_id from issue_labels where label_id = {}",
                label_id
            ))? {
                issue_ids.push(result?.values[0].u64().unwrap().to_string());
            }
            query.push_str(&format!(" and id in ({})", issue_ids.join(", ")));
        }
        query.write_str(
            format!(
                "order by created_at desc limit {} offset {};",
                args.page_num,
                (args.page - 1) * args.page_num
            )
            .as_str(),
        )?;
        let iter = self.database.run(query)?;
        let schema = iter.schema().clone();
        Ok(iter.map(move |result| {
            result
                .map(|tuple| Issue::from((&schema, tuple)))
                .map_err(anyhow::Error::from)
        }))
    }

    async fn update_issues(&self, args: &UpdateArgs) -> anyhow::Result<()> {
        let iter = self.database.run("select * from repos")?;
        let schema = iter.schema().clone();

        for tuple in iter {
            let repo = Repo::from((&schema, tuple?));

            let page = 1;
            let created_after = if let Some(datetime) = args.create_after {
                datetime.timestamp()
            } else {
                let now = Utc::now();
                let today_midnight = Utc.ymd(now.year(), now.month(), now.day()).and_hms(0, 0, 0);

                today_midnight.timestamp()
            };
            let mut oldest_created = None;
            while oldest_created
                .as_ref()
                .map(|created| *created > created_after)
                .unwrap_or(true)
            {
                let url = format!(
                    "https://api.github.com/repos/{}/issues?page={}",
                    repo.full_name(),
                    page,
                );

                let response = self
                    .client
                    .get(&url)
                    .header("User-Agent", "reqwest")
                    .send()
                    .await?;

                if !response.status().is_success() {
                    return Err(anyhow!("Request failed with status: {}", response.status()));
                }
                for mut issue in response.json::<Vec<Issue>>().await? {
                    issue.repo_name = repo.full_name();
                    issue.insert(&self.database)?;

                    let issue_created_at = issue.created_at.timestamp();
                    match oldest_created {
                        None => oldest_created = Some(issue_created_at),
                        Some(timestamp) => {
                            oldest_created = Some(min(issue_created_at, timestamp));
                        }
                    }
                }
            }
        }
        Ok(())
    }

    fn add_repo(&self, repo: &Repo) -> anyhow::Result<()> {
        repo.insert(&self.database)?;

        Ok(())
    }

    fn remove_repo(&self, repo: &Repo) -> anyhow::Result<()> {
        repo.delete(&self.database)?;

        Ok(())
    }

    fn repos(&self) -> anyhow::Result<impl Iterator<Item = Result<Repo, anyhow::Error>> + use<'_>> {
        let iter = self.database.run("select * from repos;")?;
        let schema = iter.schema().clone();
        Ok(iter.map(move |result| {
            result
                .map(|tuple| Repo::from((&schema, tuple)))
                .map_err(anyhow::Error::from)
        }))
    }
}
