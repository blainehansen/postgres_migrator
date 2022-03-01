use std::{io::{self, Read, Write}, fs, path::PathBuf};
use chrono::{Utc};

type SomeError = Box<dyn std::error::Error>;
type SomeResult<T> = Result<T, SomeError>;

fn create_timestamp() -> String {
	Utc::now().format("%Y%m%d%H%M%S").to_string()
}

#[test]
fn test_create_timestamp() {
	assert_eq!(create_timestamp().len(), 14);
}


fn ensure_migrations_directory() -> io::Result<()> {
	fs::create_dir_all("./migrations")
}

fn purge_migrations_directory() -> io::Result<()> {
	let migrations_dir = PathBuf::from("./migrations");
	match migrations_dir.exists() {
		true => fs::remove_dir_all(migrations_dir),
		false => Ok(()),
	}
}

#[test]
#[serial_test::serial]
fn test_ensure_migrations_directory() -> io::Result<()> {
	purge_migrations_directory()?;
	ensure_migrations_directory()?;
	ensure_migrations_directory()?;
	purge_migrations_directory()?;
	purge_migrations_directory()?;
	ensure_migrations_directory()?;
	ensure_migrations_directory()?;
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


fn list_migration_files() -> io::Result<Vec<PathBuf>> {
	let mut entries = vec![];
	let sql_extension = Some(std::ffi::OsStr::new("sql"));

	for entry in fs::read_dir("./migrations")? {
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
fn test_list_migration_files() -> io::Result<()> {
	purge_migrations_directory()?;
	ensure_migrations_directory()?;

	fs::File::create("./migrations/30_yo.sql")?;
	fs::File::create("./migrations/10_yo.sql")?;
	fs::create_dir("./migrations/yoyo.sql")?;
	fs::File::create("./migrations/20_yo.sql")?;
	fs::File::create("./migrations/40.txt")?;
	fs::File::create("./migrations/yo")?;
	fs::create_dir("./migrations/agh")?;

	let migration_files = list_migration_files()?;
	assert_eq!(migration_files, vec![
		PathBuf::from("./migrations/10_yo.sql"),
		PathBuf::from("./migrations/20_yo.sql"),
		PathBuf::from("./migrations/30_yo.sql"),
	]);

	purge_migrations_directory()?;
	Ok(())
}


fn compute_diff(source_dbname: &str, target_dbname: &str) -> pyo3::PyResult<String> {
	use pyo3::prelude::*;
	Python::with_gil(|py| {
		let get_diff: Py<PyAny> = PyModule::from_code(
			py,
			"def get_diff(source_dbname, target_dbname):
				import migra
				migration = migra.Migration(
					source_dbname,
					target_dbname,
					# self.config.database.schema,
				)
				migration.set_safety(False)
				migration.add_all_changes(privileges=True)
				return migration.sql",
			"", "",
		)?.getattr("get_diff")?.into();

		get_diff.call1(py, (source_dbname, target_dbname))?.extract(py)
	})
}


fn command_migrate(raw_description: &str) -> SomeResult<()> {
	let description_slug = make_slug(raw_description);
	let timestamp = create_timestamp();

	let migration_up = compute_diff("migrations", "schema")?;

	ensure_migrations_directory()?;
	fs::File::create(format!("./migrations/{timestamp}.{description_slug}.sql"))?
		.write_all(migration_up.as_bytes())?;

	Ok(())
}


fn command_compact(client: &mut postgres::Client, source_dbname: &str, target_dbname: &str) -> SomeResult<()> {
	command_migrate("ensuring_current", source_dbname, target_dbname)?;
	command_up(client)?;

	purge_migrations_directory()?;
	ensure_migrations_directory()?;
	command_migrate("compacted_initial", source_dbname, target_dbname)?;
	let migration_files = list_migration_files()?;
	let version = migration_files[0].to_str().unwrap().split('.').nth(0).unwrap();
	println!("new version number is: {version}", );

	client.batch_execute(&format!("truncate table _schema_versions; insert into _schema_versions (version) values ({version})"))?;
	Ok(())
}


fn command_up(client: &mut postgres::Client) -> SomeResult<()> {
	client.batch_execute("create table if not exists _schema_versions (version char(14) unique not null)")?;

	let current_version: Option<String> = client
		.query_opt("select max(version) as current from _schema_versions", &[])?
		.map(|row| row.get("current"));
	// dbg!(current_version);

	ensure_migrations_directory()?;
	for migration_file in list_migration_files()? {
		// let migration_file = migration_file.to_str().ok_or(Err(format!("not valid unicode: {migration_file}")))?;
		let migration_file = migration_file.to_str().unwrap();
		// let version = migration_file.split(".").nth(0).ok_or(Err(format!("doesn't have a version: {migration_file}")))?;
		let version = migration_file.split(".").nth(0).unwrap();
		let mut perform_migration = || -> SomeResult<()> {
			println!("performing {migration_file}");
			let mut file = fs::File::open(migration_file)?;
			let mut migration_query = String::new();
			file.read_to_string(&mut migration_query)?;

			client.batch_execute(&format!("{migration_query}; INSERT into _schema_versions (version) values ({version})"))?;
			Ok(())
		};

		match current_version {
			None => perform_migration()?,
			Some(ref current_version) if version > current_version.as_str() => perform_migration()?,
			_ => println!("not performing {migration_file}"),
		}
	}

	Ok(())
}

fn command_clean(main_config: &postgres::Config) -> SomeResult<()> {
	let mut main_client = main_config.clone().dbname("template1").connect(postgres::NoTls)?;
	let query = format!("
		select databases.datname as dbname
		from
			pg_database as databases
			join pg_shdescription as descriptions on descriptions.objoid = databases.oid
		where descriptions.description = {TEMP_DB_COMMENT}
	");
	for row in main_client.query(&query, &[])? {
		let dbname: String = row.get("dbname");
		main_client.batch_execute(&format!("drop database if exists {dbname}"))?;
	}

	Ok(())
}


fn command_diff(arg: Type) -> RetType {
	unimplemented!()
}


const TEMP_DB_COMMENT: &'static str = "'TEMP DB CREATED BY migrator'";

struct TempDb {
	dbname: String,
	main_config: postgres::Config,
}

impl TempDb {
	fn new(dbname: &str, suffix: &str, main_config: &postgres::Config) -> SomeResult<TempDb> {
		let now = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_secs();
		let dbname = format!("{dbname}_{now}_{suffix}");

		let mut main_client = main_config.clone().dbname("template1").connect(postgres::NoTls)?;
		main_client.execute(&format!(
			"create database {dbname}"
		), &[])?;
		main_client.batch_execute(&format!(
			"comment on database {dbname} is {TEMP_DB_COMMENT}"
		))?;

		Ok(TempDb{dbname, main_config: main_config.clone()})
	}
}

impl Drop for TempDb {
	fn drop(&mut self) {
		let dbname = &self.dbname;
		let mut main_client = self.main_config.connect(postgres::NoTls).unwrap();
		main_client.batch_execute(&format!("drop database if exists {dbname}")).unwrap();
	}
}


use clap::{Parser, Subcommand};

#[derive(Parser, Debug)]
#[clap(author, version)]
struct Args {
	#[clap(subcommand)]
	command: Commands,
}

#[derive(Subcommand, Debug)]
enum Commands {
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
	Diff {
		source: Backends,
		target: Backends,
	},

	// Check,
}

#[derive(Debug)]
enum Backends {
	Migrations,
	Schema,
	Database,
}

impl Backends {
	fn to_suffix(&self) -> &'static str {
		match self {
			Migrations => "migrations",
			Schema => "schema",
			Database => "database",
		}
	}
}


fn main() -> SomeResult<()> {
	let args = Args::parse();
	dbg!(args);
	let main_dbname = "experiment_db";
	let mut main_config = postgres::Config::new();
	main_config
		.user("experiment_user")
		.password("asdf")
		.host("localhost")
		.port(5432)
		.dbname(main_dbname)
		.ssl_mode(postgres::config::SslMode::Disable);

	match args.command {
		Migrate{migration_description} => {
			command_migrate(migration_description)?;
		},
		Up => {
			command_up()?;
		},
		Clean => {
			command_clean(main_config)?;
		},
		Compact => {
			command_compact(main_config, source_dbname, target_dbname)?;
		},
		Diff{source, target} => {
			let source_db = source.to_db()?;
			let target_db = target.to_db()?;

			let diff = compute_diff(source_db.dbname(), target_db.dbname())?;
			println!("{diff}");
		},
	}

	// let migrations_temp = TempDb::new(main_dbname, "migrations", &main_config)?;
	// println!("migrations_temp: {}", &migrations_temp.dbname);
	// let schema_temp = TempDb::new(main_dbname, "schema", &main_config)?;
	// println!("schema_temp: {}", &schema_temp.dbname);

	Ok(())
}
