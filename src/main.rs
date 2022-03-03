use std::{io::{self, Read, Write}, fs, path::PathBuf};
use chrono::Utc;
use postgres::Config;
use anyhow::{anyhow, Result};

fn create_timestamp() -> String {
	Utc::now().format("%Y%m%d%H%M%S").to_string()
}

#[test]
fn test_create_timestamp() {
	assert_eq!(create_timestamp().len(), 14);
}


fn ensure_migrations_directory(migrations_directory: &str) -> io::Result<()> {
	fs::create_dir_all(migrations_directory)
}

fn purge_migrations_directory(migrations_directory: &str) -> io::Result<()> {
	let migrations_directory = PathBuf::from(migrations_directory);
	match migrations_directory.exists() {
		true => fs::remove_dir_all(migrations_directory),
		false => Ok(()),
	}
}

const DEFAULT_MIGRATIONS_DIRECTORY: &'static str = "migrations";
const DEFAULT_SCHEMA_DIRECTORY: &'static str = "schema";

#[test]
#[serial_test::serial]
fn test_ensure_migrations_directory() -> io::Result<()> {
	purge_migrations_directory(DEFAULT_MIGRATIONS_DIRECTORY)?;
	ensure_migrations_directory(DEFAULT_MIGRATIONS_DIRECTORY)?;
	ensure_migrations_directory(DEFAULT_MIGRATIONS_DIRECTORY)?;
	purge_migrations_directory(DEFAULT_MIGRATIONS_DIRECTORY)?;
	purge_migrations_directory(DEFAULT_MIGRATIONS_DIRECTORY)?;
	ensure_migrations_directory(DEFAULT_MIGRATIONS_DIRECTORY)?;
	ensure_migrations_directory(DEFAULT_MIGRATIONS_DIRECTORY)?;
	Ok(())
}


fn make_slug(text: &str) -> String {
	let re = regex::Regex::new(r"\W+").unwrap();
	re.replace_all(text, "_").to_lowercase().into()
}

#[test]
fn test_make_slug() {
	assert_eq!(make_slug("yo yo"), "yo_yo");
	assert_eq!(make_slug("Hello, World!"), "hello_world_");
	assert_eq!(make_slug("Hello, World"), "hello_world");
	assert_eq!(make_slug("1, 2, yoyo, World"), "1_2_yoyo_world");
}


fn list_sql_files(directory: &str) -> io::Result<Vec<PathBuf>> {
	let mut entries = vec![];
	let sql_extension = Some(std::ffi::OsStr::new("sql"));

	for entry in fs::read_dir(directory)? {
		let entry = entry?;
		let path = entry.path();
		if !path.is_dir() && path.extension() == sql_extension {
			entries.push(path);
		}
	}
	entries.sort();
	Ok(entries)
}

#[test]
#[serial_test::serial]
fn test_list_sql_files() -> io::Result<()> {
	purge_migrations_directory(DEFAULT_MIGRATIONS_DIRECTORY)?;
	ensure_migrations_directory(DEFAULT_MIGRATIONS_DIRECTORY)?;

	fs::File::create("migrations/30_yo.sql")?;
	fs::File::create("migrations/10_yo.sql")?;
	fs::create_dir("migrations/yoyo.sql")?;
	fs::File::create("migrations/20_yo.sql")?;
	fs::File::create("migrations/40.txt")?;
	fs::File::create("migrations/yo")?;
	fs::create_dir("migrations/agh")?;

	let migration_files = list_sql_files(DEFAULT_MIGRATIONS_DIRECTORY)?;
	assert_eq!(migration_files, vec![
		PathBuf::from("migrations/10_yo.sql"),
		PathBuf::from("migrations/20_yo.sql"),
		PathBuf::from("migrations/30_yo.sql"),
	]);

	purge_migrations_directory(DEFAULT_MIGRATIONS_DIRECTORY)?;
	Ok(())
}


fn to_connection_string(config: &Config) -> String {
	let user_string = match (config.get_user(), config.get_password()) {
		(None, None) | (None, Some(_)) => "".to_string(),
		(Some(user), None) => format!("{user}@"),
		(Some(user), Some(password)) => format!("{user}:{}@", std::str::from_utf8(password).unwrap()),
	};
	let localhost = postgres::config::Host::Tcp("localhost".to_string());
	let host = match config.get_hosts().first().unwrap_or(&localhost) {
		postgres::config::Host::Tcp(v) => v,
		postgres::config::Host::Unix(v) => v.to_str().unwrap(),
	};
	let port = config.get_ports().first().unwrap_or(&5432);
	let dbname = config.get_dbname().unwrap_or("");
	format!("postgresql://{user_string}{host}:{port}/{dbname}")
}

#[test]
fn test_to_connection_string() {
	let mut config = Config::new();
	assert_eq!(to_connection_string(&config), "postgresql:://localhost:5432/");

	config.dbname("template1");
	config.host("db");
	config.port(1111);
	assert_eq!(to_connection_string(&config), "postgresql:://db:1111/template1");

	config.user("user");
	assert_eq!(to_connection_string(&config), "postgresql:://user@db:1111/template1");

	config.password("password");
	assert_eq!(to_connection_string(&config), "postgresql:://user:password@db:1111/template1");

	let mut config = Config::new();
	config.password("password");
	config.dbname("template1");
	config.port(1111);
	assert_eq!(to_connection_string(&config), "postgresql:://localhost:1111/template1");
}


fn compute_diff(source: &Config, target: &Config) -> Result<String> {
	let output = std::process::Command::new("migra")
		.arg("--unsafe")
		.arg("--with-privileges")
		.arg(to_connection_string(source))
		.arg(to_connection_string(target))
		.output()?;

	if output.stderr.len() != 0 {
		return Err(anyhow!("migra failed: {}\n\n{}", output.status, String::from_utf8_lossy(&output.stderr)));
	}
	Ok(String::from_utf8_lossy(&output.stdout).into_owned())
}


fn apply_sql_files(config: &Config, directory: &str) -> Result<()> {
	let mut client = config.connect(postgres::NoTls)?;
	for sql_file in list_sql_files(directory)? {
		let mut file = fs::File::open(sql_file)?;
		let mut query = String::new();
		file.read_to_string(&mut query)?;
		client.batch_execute(&query)?;
	}

	Ok(())
}


fn command_migrate(args: &Args, base_config: &Config, raw_description: &str) -> Result<String> {
	let dbname = args.dbname.as_ref().ok_or(anyhow!("need a dbname to run migrate command"))?;
	let description_slug = make_slug(raw_description);
	let version = create_timestamp();

	let source = TempDb::new(&dbname, "migrations", &base_config)?;
	apply_sql_files(&source.config, &args.migrations_directory)?;
	let target = TempDb::new(&dbname, "schema", &base_config)?;
	apply_sql_files(&target.config, &args.schema_directory)?;

	let migration_up = compute_diff(&source.config, &target.config)?;

	ensure_migrations_directory(&args.migrations_directory)?;
	fs::File::create(format!("./{}/{version}.{description_slug}.sql", args.migrations_directory))?
		.write_all(migration_up.as_bytes())?;

	Ok(version)
}


fn command_compact(args: &Args, base_config: &Config) -> Result<()> {
	let mut client = base_config.connect(postgres::NoTls)?;
	command_migrate(args, base_config, "ensuring_current")?;
	command_up(args, &mut client)?;

	purge_migrations_directory(&args.migrations_directory)?;
	ensure_migrations_directory(&args.migrations_directory)?;
	let version = command_migrate(args, base_config, "compacted_initial")?;
	println!("new version number is: {version}");

	client.batch_execute(&format!("truncate table _schema_versions; insert into _schema_versions (version) values ({version})"))?;
	Ok(())
}


fn command_up(args: &Args, client: &mut postgres::Client) -> Result<()> {
	client.batch_execute("create table if not exists _schema_versions (version char(14) unique not null)")?;

	let current_version: Option<String> = client
		.query_one("select max(version) as current from _schema_versions", &[])?
		.get("current");

	ensure_migrations_directory(&args.migrations_directory)?;
	for migration_file in list_sql_files(&args.migrations_directory)? {
		let version = migration_file.file_name()
			.and_then(|name| name.to_str())
			.and_then(|name| name.split(".").nth(0))
			.ok_or(anyhow!("unable to determine version: {:?}", migration_file))?;
		let migration_file = migration_file.to_str().ok_or(anyhow!("not valid unicode: {:?}", migration_file))?;
		let mut perform_migration = || -> Result<()> {
			println!("performing {:?}", migration_file);
			let mut file = fs::File::open(migration_file)?;
			let mut migration_query = String::new();
			file.read_to_string(&mut migration_query)?;

			client.batch_execute(&format!("{migration_query}; INSERT into _schema_versions (version) values ({version})"))?;
			Ok(())
		};

		match current_version {
			None => perform_migration()?,
			Some(ref current_version) if version > current_version.as_str() => perform_migration()?,
			_ => println!("not performing {:?}", migration_file),
		}
	}

	Ok(())
}

fn command_clean(mut base_config: Config) -> Result<()> {
	let mut client = base_config.dbname("template1").connect(postgres::NoTls)?;
	let query = format!("
		select databases.datname as dbname
		from
			pg_database as databases
			join pg_shdescription as descriptions on descriptions.objoid = databases.oid
		where descriptions.description = {TEMP_DB_COMMENT}
	");
	for row in client.query(&query, &[])? {
		let dbname: String = row.get("dbname");
		client.batch_execute(&format!("drop database if exists {dbname}"))?;
	}

	Ok(())
}


fn ensure_db(args: &Args, dbname: &str, base_config: &Config, backend: Backend) -> Result<(Option<TempDb>, Config)> {
	match backend {
		Backend::Migrations => {
			let temp = TempDb::new(dbname, "migrations", base_config)?;
			apply_sql_files(&temp.config, &args.migrations_directory)?;
			let config = temp.config.clone();
			Ok((Some(temp), config))
		},
		Backend::Schema => {
			let temp = TempDb::new(dbname, "schema", base_config)?;
			apply_sql_files(&temp.config, &args.schema_directory)?;
			let config = temp.config.clone();
			Ok((Some(temp), config))
		},
		Backend::Database => Ok((None, base_config.clone())),
	}
}

fn command_diff(args: &Args, base_config: &Config, source: Backend, target: Backend) -> Result<()> {
	if source == target {
		return Err(anyhow!("can't diff {:?} against itself", source))
	}

	let dbname = args.dbname.as_ref().ok_or(anyhow!("need a dbname to run migrate command"))?;
	let source = ensure_db(args, dbname, base_config, source)?;
	let target = ensure_db(args, dbname, base_config, target)?;

	let diff = compute_diff(&source.1, &target.1)?;
	println!("{diff}");

	Ok(())
}


const TEMP_DB_COMMENT: &'static str = "'TEMP DB CREATED BY migrator'";

struct TempDb {
	dbname: String,
	config: Config,
}

impl TempDb {
	fn new(dbname: &str, suffix: &str, base_config: &Config) -> Result<TempDb> {
		let now = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_secs();
		let dbname = format!("{dbname}_{now}_{suffix}");

		let mut config = base_config.clone();
		config.dbname(&dbname);

		let mut client = base_config.clone().dbname("template1").connect(postgres::NoTls)?;
		client.execute(&format!("create database {dbname}"), &[])?;
		client.batch_execute(&format!("comment on database {dbname} is {TEMP_DB_COMMENT}"))?;

		Ok(TempDb{dbname, config})
	}
}

impl Drop for TempDb {
	fn drop(&mut self) {
		let dbname = &self.dbname;

		let _ = self.config.dbname("template1").connect(postgres::NoTls)
			.map_err(|err| { eprintln!("unable to drop {dbname}: {err}"); err })
			.and_then(|mut client| {
				client.batch_execute(&format!("drop database if exists {dbname}"))
			})
			.map_err(|err| { eprintln!("unable to drop {dbname}: {err}"); err });
	}
}

use clap::Parser;

#[derive(Parser, Debug)]
#[clap(author, version)]
struct Args {
	#[clap(long)]
	user: Option<String>,
	#[clap(long)]
	password: Option<String>,
	#[clap(long, default_value_t = String::from("localhost"))]
	host: String,
	#[clap(long, default_value_t = 5432)]
	port: u16,
	#[clap(long)]
	dbname: Option<String>,

	#[clap(long, default_value_t = String::from(DEFAULT_SCHEMA_DIRECTORY))]
	schema_directory: String,
	#[clap(long, default_value_t = String::from(DEFAULT_MIGRATIONS_DIRECTORY))]
	migrations_directory: String,

	#[clap(subcommand)]
	command: Command,
}

#[derive(clap::Subcommand, Debug)]
enum Command {
	/// cleans the current instance of all temporary databases
	Clean,

	/// apply all migrations to database
	Up,
	/// ensure both database and migrations folder are current with schema, and compact to only one migration
	Compact,
	/// generate new migration and place in migrations folder
	Migrate {
		/// description of migration, will be converted to "snake_case"
		migration_description: String,
	},
	/// prints out the sql diff necessary to convert `source` to `target`
	Diff {
		#[clap(arg_enum)]
		source: Backend,
		#[clap(arg_enum)]
		target: Backend,
	},

	// Check,
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord, clap::ArgEnum)]
enum Backend {
	Migrations,
	Schema,
	Database,
}


fn main() -> Result<()> {
	let args = Args::parse();
	let base_config = {
		let mut base_config = Config::new();
		if let Some(ref user) = args.user { base_config.user(&user); }
		if let Some(ref password) = args.password { base_config.password(&password); }
		if let Some(ref dbname) = args.dbname { base_config.dbname(&dbname); }
		base_config
			.host(&args.host)
			.port(args.port)
			.ssl_mode(postgres::config::SslMode::Disable);
		base_config
	};

	match args.command {
		Command::Migrate{ref migration_description} => {
			command_migrate(&args, &base_config, &migration_description)?;
		},
		Command::Up => {
			let mut client = base_config.connect(postgres::NoTls)?;
			command_up(&args, &mut client)?;
		},
		Command::Clean => {
			command_clean(base_config)?;
		},
		Command::Compact => {
			command_compact(&args, &base_config)?;
		},
		Command::Diff{source, target} => {
			command_diff(&args, &base_config, source, target)?;
		},
	}

	Ok(())
}
