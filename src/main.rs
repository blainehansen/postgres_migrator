use std::{io::{self, Read, Write}, fs, path::PathBuf};
use chrono::Utc;
use postgres::Config;
use anyhow::{anyhow, Result, Context};

fn create_timestamp() -> String {
	Utc::now().format("%Y%m%d%H%M%S").to_string()
}

#[test]
fn test_create_timestamp() {
	assert_eq!(create_timestamp().len(), 14);
}

fn get_null_string() -> String {
	"null".to_string()
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


#[derive(Debug, Eq, PartialEq)]
struct MigrationFile {
	file_path: PathBuf,
	display_file_path: String,
	current_version: String,
	previous_version: String,
}

impl MigrationFile {
	/// file_paths is expected to be sorted alphanumerically
	fn vec_from_paths(file_paths: Vec<PathBuf>) -> Result<Vec<MigrationFile>> {
		let mut migration_files = vec![];
		let mut last_seen_current_version = get_null_string();

		for (index, file_path) in file_paths.into_iter().enumerate() {
			let display_file_path = file_path.to_string_lossy().to_string();

			// first parse the file_name and version strings
			let file_name = file_path.file_name().ok_or_else(|| anyhow!("no file name forst this path: {display_file_path}"))?;
			let file_name = file_name.to_str().ok_or_else(|| anyhow!("file name isn't valid unicode: {display_file_path}"))?;
			let mut portions = file_name.split(".");
			let current_version = portions.next()
				.ok_or_else(|| anyhow!("no version strings in this path: {display_file_path}"))?.to_string();
			let previous_version = portions.next()
				.ok_or_else(|| anyhow!("no previous version string in this path: {display_file_path}"))?.to_string();

			// then check that the version strings align with the previous one
			if previous_version != last_seen_current_version {
				return Err(anyhow!("misaligned versions in {display_file_path}: expected {last_seen_current_version}, got {previous_version}"));
			}
			last_seen_current_version = current_version.clone();

			let validate_version_string = |version_string: String| {
				match version_string.len() {
					14 => Ok(version_string),
					_ => Err(anyhow!("{version_string} is supposed to have exactly 14 characters")),
				}
			};
			let current_version = validate_version_string(current_version)?;
			let previous_version = match previous_version == "null" {
				true => {
					// check that nulls are only allowed in the first spot
					if !(index == 0) {
						return Err(anyhow!("null previous_version in migration that isn't the first: {display_file_path}"));
					}
					previous_version
				},
				false => {
					let previous_version = previous_version;
					if !(current_version > previous_version) {
						return Err(anyhow!("all migration versions have to be sequential, so {current_version} must be greater than {previous_version}"));
					}
					validate_version_string(previous_version)?
				}
			};

			migration_files.push(MigrationFile{file_path, display_file_path, current_version, previous_version});
		}

		Ok(migration_files)
	}
}

#[test]
fn test_migration_files_vec_from_paths() {
	let ex = |file_path: PathBuf, current_version: &str, previous_version: &str| {
		let display_file_path = file_path.to_string_lossy().to_string();
		MigrationFile{file_path, display_file_path, current_version: current_version.to_string(), previous_version: previous_version.to_string()}
	};
	let version = create_timestamp();

	assert!(MigrationFile::vec_from_paths(vec![PathBuf::from("err/short.sql")]).is_err());
	assert!(MigrationFile::vec_from_paths(vec![PathBuf::from("err/short.short.sql")]).is_err());
	assert!(MigrationFile::vec_from_paths(vec![PathBuf::from(format!("err/{version}.{version}.sql"))]).is_err());
	assert!(MigrationFile::vec_from_paths(vec![PathBuf::from(format!("err/null.{version}.sql"))]).is_err());
	assert!(MigrationFile::vec_from_paths(vec![
		PathBuf::from(format!("err/{version}.null.sql")),
		PathBuf::from(format!("err/90000000000000.null.sql")),
	]).is_err());
	assert!(MigrationFile::vec_from_paths(vec![
		PathBuf::from(format!("err/{version}.null.sql")),
		PathBuf::from(format!("err/null.{version}.sql")),
	]).is_err());

	assert_eq!(MigrationFile::vec_from_paths(vec![]).unwrap(), vec![]);

	let file_path = PathBuf::from(format!("ok/{version}.null.sql"));
	assert_eq!(
		MigrationFile::vec_from_paths(vec![file_path.clone()]).unwrap(),
		vec![ex(file_path, &version, "null")],
	);

	let file_path1 = PathBuf::from(format!("ok/{version}.null.sql"));
	let file_path2 = PathBuf::from(format!("ok/90000000000000.{version}.sql"));
	let file_path3 = PathBuf::from(format!("ok/90000000000001.90000000000000.sql"));
	let file_path4 = PathBuf::from(format!("ok/90000000000002.90000000000001.sql"));
	assert_eq!(
		MigrationFile::vec_from_paths(vec![file_path1.clone(), file_path2.clone(), file_path3.clone(), file_path4.clone()]).unwrap(),
		vec![
			ex(file_path1, &version, "null"),
			ex(file_path2, "90000000000000", &version),
			ex(file_path3, "90000000000001", "90000000000000"),
			ex(file_path4, "90000000000002", "90000000000001"),
		],
	);
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
	assert_eq!(to_connection_string(&config), "postgresql://localhost:5432/");

	config.dbname("template1");
	config.host("db");
	config.port(1111);
	assert_eq!(to_connection_string(&config), "postgresql://db:1111/template1");

	config.user("user");
	assert_eq!(to_connection_string(&config), "postgresql://user@db:1111/template1");

	config.password("password");
	assert_eq!(to_connection_string(&config), "postgresql://user:password@db:1111/template1");

	let mut config = Config::new();
	config.password("password");
	config.dbname("template1");
	config.port(1111);
	assert_eq!(to_connection_string(&config), "postgresql://localhost:1111/template1");
}


fn config_try_from_str(pg_url: &str) -> std::result::Result<Config, postgres::Error> {
	pg_url.parse::<Config>()
}

#[test]
fn test_config_try_from_str() {
	assert!(config_try_from_str("yoyoyo").is_err());

	assert_eq!(
		to_connection_string(&config_try_from_str("postgresql://localhost:5432/").unwrap()),
		to_connection_string(Config::new().host("localhost").port(5432)),
	);

	assert_eq!(
		to_connection_string(&config_try_from_str("postgresql://db:1111/template1").unwrap()),
		to_connection_string(Config::new().host("db").port(1111).dbname("template1")),
	);

	assert_eq!(
		to_connection_string(&config_try_from_str("postgresql://user@db:1111/template1").unwrap()),
		to_connection_string(Config::new().user("user").host("db").port(1111).dbname("template1")),
	);

	assert_eq!(
		to_connection_string(&config_try_from_str("postgresql://user:password@db:1111/template1").unwrap()),
		to_connection_string(Config::new().user("user").password("password").host("db").port(1111).dbname("template1")),
	);

	assert_eq!(
		to_connection_string(&config_try_from_str("postgresql://localhost:1111/template1").unwrap()),
		to_connection_string(Config::new().host("localhost").port(1111).dbname("template1")),
	);
}


fn gather_validated_migrations(args: &Args, client: &mut postgres::Client) -> Result<(Vec<MigrationFile>, Option<String>)> {
	client.batch_execute("
		create table if not exists _schema_versions (
			current_version char(14) not null unique,
			previous_version char(14) references _schema_versions(current_version) unique,
			check (current_version > previous_version)
		);
		create unique index if not exists i_schema_versions on _schema_versions ((previous_version is null)) where previous_version is null
	")?;

	ensure_migrations_directory(&args.migrations_directory)?;
	let migration_files = MigrationFile::vec_from_paths(list_sql_files(&args.migrations_directory)?)?;

	let current_version = migration_files.last().map(|migration_file| migration_file.current_version.clone());

	Ok((migration_files, current_version))
}


fn compute_diff(source: &Config, target: &Config) -> Result<String> {
	let output = std::process::Command::new("migra")
		.arg("--unsafe")
		.arg("--with-privileges")
		.arg(to_connection_string(source))
		.arg(to_connection_string(target))
		.output()
		.context("Error while calling migra")?;

	if output.stderr.len() != 0 {
		return Err(anyhow!("migra failed: {}\n\n{}", output.status, String::from_utf8_lossy(&output.stderr)));
	}
	Ok(String::from_utf8_lossy(&output.stdout).trim().to_string())
}


fn apply_sql_files(config: &Config, sql_files: Vec<PathBuf>) -> Result<()> {
	let mut client = config.connect(postgres::NoTls)?;
	for sql_file in sql_files {
		let mut file = fs::File::open(sql_file)?;
		let mut query = String::new();
		file.read_to_string(&mut query)?;
		client.batch_execute(&query)?;
	}

	Ok(())
}


fn command_generate(args: &Args, raw_description: &str) -> Result<String> {
	let mut client = args.pg_url.connect(postgres::NoTls)?;
	let dbname = args.pg_url.get_dbname().ok_or(anyhow!("need a dbname to run generate command"))?;
	let (migration_files, previous_version) = gather_validated_migrations(&args, &mut client)?;
	let previous_version = previous_version.unwrap_or_else(get_null_string);

	let description_slug = make_slug(raw_description);
	let current_version = create_timestamp();

	let source = TempDb::new(&dbname, "migrations", &args.pg_url)?;
	apply_sql_files(&source.config, migration_files.into_iter().map(|migration_file| migration_file.file_path).collect())?;
	let target = TempDb::new(&dbname, "schema", &args.pg_url)?;
	apply_sql_files(&target.config, list_sql_files(&args.schema_directory)?)?;

	let generated_migration = compute_diff(&source.config, &target.config)?;

	fs::File::create(format!("./{}/{current_version}.{previous_version}.{description_slug}.sql", args.migrations_directory))?
		.write_all(generated_migration.as_bytes())?;

	Ok(current_version)
}


fn command_compact(args: &Args) -> Result<()> {
	let mut client = args.pg_url.connect(postgres::NoTls)?;
	command_generate(args, "ensuring_current")?;
	command_migrate(args, &mut client)?;

	purge_migrations_directory(&args.migrations_directory)?;
	ensure_migrations_directory(&args.migrations_directory)?;
	let current_version = command_generate(args, "compacted_initial")?;
	println!("new version number is: {current_version}");

	client.batch_execute(&format!("
		truncate table _schema_versions;
		insert into _schema_versions (current_version, previous_version) values ({current_version}, null)
	"))?;
	Ok(())
}


fn command_migrate(args: &Args, client: &mut postgres::Client) -> Result<()> {
	let migration_files = gather_validated_migrations(&args, client)?.0;

	let actual_version: Option<String> = client
		.query_one("select max(current_version) as current_version from _schema_versions", &[])?
		.get("current_version");

	for MigrationFile{display_file_path, file_path, current_version, previous_version} in migration_files {
		let mut perform_migration = || -> Result<()> {
			println!("performing {}", display_file_path);
			let mut file = fs::File::open(&file_path)?;
			let mut migration_query = String::new();
			file.read_to_string(&mut migration_query)?;

			let mut transaction = client.transaction()?;
			transaction.batch_execute(&migration_query)?;
			transaction.batch_execute(&format!("
				insert into _schema_versions (current_version, previous_version) values ({current_version}, {previous_version})
			"))?;
			transaction.commit()?;

			Ok(())
		};

		match actual_version {
			None => perform_migration()?,
			Some(ref actual_version) if &current_version > actual_version => perform_migration()?,
			_ => println!("not performing {}", display_file_path),
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
			apply_sql_files(&temp.config, list_sql_files(&args.migrations_directory)?)?;
			let config = temp.config.clone();
			Ok((Some(temp), config))
		},
		Backend::Schema => {
			let temp = TempDb::new(dbname, "schema", base_config)?;
			apply_sql_files(&temp.config, list_sql_files(&args.schema_directory)?)?;
			let config = temp.config.clone();
			Ok((Some(temp), config))
		},
		Backend::Database => Ok((None, base_config.clone())),
	}
}

fn compute_backend_diff(args: &Args, source: Backend, target: Backend) -> Result<String> {
	if source == target {
		return Err(anyhow!("can't diff {:?} against itself", source))
	}

	let dbname = args.pg_url.get_dbname().ok_or(anyhow!("provided pg_url has no dbname"))?;
	let source = ensure_db(args, dbname, &args.pg_url, source)?;
	let target = ensure_db(args, dbname, &args.pg_url, target)?;
	Ok(compute_diff(&source.1, &target.1)?)
}

fn command_diff(args: &Args, source: Backend, target: Backend) -> Result<()> {
	let diff = compute_backend_diff(&args, source, target)?;
	println!("{diff}");
	Ok(())
}

fn command_check(args: &Args, source: Backend, target: Backend) -> Result<()> {
	let diff = compute_backend_diff(&args, source, target)?;
	if !diff.is_empty() {
		return Err(anyhow!("diff isn't empty:\n\n{diff}"))
	}
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
	/// postgres connection string, in the form postgres://user:password@host:port/database
	/// can also be loaded from the environment variable PG_URL
	#[clap(long, env = "PG_URL", parse(try_from_str = config_try_from_str))]
	pg_url: Config,

	/// directory where the declarative schema is located
	#[clap(long, default_value_t = String::from(DEFAULT_SCHEMA_DIRECTORY))]
	schema_directory: String,
	/// directory where migrations are stored
	#[clap(long, default_value_t = String::from(DEFAULT_MIGRATIONS_DIRECTORY))]
	migrations_directory: String,

	#[clap(subcommand)]
	command: Command,
}

#[derive(clap::Subcommand, Debug)]
enum Command {
	/// generate new migration and place in migrations folder
	Generate {
		/// description of migration, will be converted to "snake_case"
		migration_description: String,
	},
	/// apply all migrations to database
	Migrate,
	/// ensure both database and migrations folder are current with schema
	/// and compact to only one migration
	Compact,

	/// checks that `source` and `target` are in sync, throws error otherwise
	Check {
		#[clap(arg_enum)]
		source: Backend,
		#[clap(arg_enum)]
		target: Backend,
	},
	/// prints out the sql diff necessary to convert `source` to `target`
	Diff {
		#[clap(arg_enum)]
		source: Backend,
		#[clap(arg_enum)]
		target: Backend,
	},

	/// cleans the current instance of all temporary databases
	Clean,
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord, clap::ArgEnum)]
enum Backend {
	Migrations,
	Schema,
	Database,
}


fn main() -> Result<()> {
	let args = Args::parse();

	match args.command {
		Command::Generate{ref migration_description} => {
			command_generate(&args, &migration_description)?;
		},
		Command::Migrate => {
			let mut client = args.pg_url.connect(postgres::NoTls)?;
			command_migrate(&args, &mut client)?;
		},
		Command::Compact => {
			command_compact(&args)?;
		},
		Command::Check{source, target} => {
			command_check(&args, source, target)?;
		},
		Command::Diff{source, target} => {
			command_diff(&args, source, target)?;
		},
		Command::Clean => {
			command_clean(args.pg_url)?;
		},
	}

	Ok(())
}

#[test]
#[serial_test::serial]
#[ignore]
fn test_full() -> Result<()> {
	fn get_config() -> Config {
		std::env::var("PG_URL").unwrap().parse::<Config>().unwrap()
	}
	fn get_args(schema_directory: &'static str) -> Args {
		Args {
			pg_url: get_config(),
			schema_directory: schema_directory.to_string(),
			migrations_directory: DEFAULT_MIGRATIONS_DIRECTORY.to_string(),
			command: Command::Clean,
		}
	}

	fn get_migration_count() -> usize {
		list_sql_files(DEFAULT_MIGRATIONS_DIRECTORY).unwrap().len()
	}

	let mut client = get_config().connect(postgres::NoTls)?;
	client.batch_execute("
		drop schema public cascade;
		create schema public;
		grant all on schema public to public;
		comment on schema public is 'standard public schema';
	")?;
	purge_migrations_directory(DEFAULT_MIGRATIONS_DIRECTORY)?;
	ensure_migrations_directory(DEFAULT_MIGRATIONS_DIRECTORY)?;

	use Backend::*;
	assert!(command_check(&get_args("schemas/schema.1"), Database, Migrations).is_ok());
	assert!(command_check(&get_args("schemas/schema.1"), Schema, Migrations).is_err());
	assert!(command_check(&get_args("schemas/schema.1"), Database, Schema).is_err());
	assert!(!compute_backend_diff(&get_args("schemas/schema.1"), Database, Schema)?.is_empty());
	assert!(compute_backend_diff(&get_args("schemas/schema.1"), Database, Migrations)?.is_empty());

	// # schema.1
	command_generate(&get_args("schemas/schema.1"), "one")?;
	assert_eq!(get_migration_count(), 1);
	command_migrate(&get_args(""), &mut get_config().connect(postgres::NoTls)?)?;
	client.batch_execute("select id, name, color from fruit")?;

	// # schema.2
	command_generate(&get_args("schemas/schema.2"), "two")?;
	assert_eq!(get_migration_count(), 2);
	command_migrate(&get_args(""), &mut get_config().connect(postgres::NoTls)?)?;
	client.batch_execute("select id, name, flavor from fruit")?;

	// # schema.3
	command_compact(&get_args("schemas/schema.3"))?;
	assert_eq!(get_migration_count(), 1);
	client.batch_execute("select person.name, fruit.name, flavor from person join fruit on person.favorite_fruit = fruit.id where flavor = 'SALTY'")?;

	// # schema.1
	command_generate(&get_args("schemas/schema.1"), "back to one")?;
	assert_eq!(get_migration_count(), 2);
	command_migrate(&get_args(""), &mut get_config().connect(postgres::NoTls)?)?;
	client.batch_execute("select id, name, color from fruit")?;

	command_clean(get_config())?;
	client.execute("create database garbage_tmp", &[])?;
	client.batch_execute("comment on database garbage_tmp is 'TEMP DB CREATED BY migrator';")?;
	command_clean(get_config())?;
	// this is just a ghetto way to make sure `clean` actually removes garbage_tmp, since this command will fail otherwise
	client.execute("create database garbage_tmp", &[])?;
	client.execute("drop database garbage_tmp", &[])?;

	Ok(())
}
