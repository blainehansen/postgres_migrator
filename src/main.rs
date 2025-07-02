use std::{fs, io::{self, Read, Write}, path::PathBuf};
use chrono::Utc;
use postgres::Config;
use anyhow::{anyhow, Result, Context};
use postgres_native_tls::MakeTlsConnector;
use native_tls::TlsConnector;
use walkdir::WalkDir;
use clap::Parser;

fn create_timestamp() -> String {
	Utc::now().format("%Y%m%d%H%M%S").to_string()
}

fn make_tls_connector() -> Result<MakeTlsConnector> {
	// Accept self-signed certificates for compatibility with cloud providers like AWS RDS
	// This is equivalent to sslmode=require in libpq
	let connector = TlsConnector::builder()
		.danger_accept_invalid_certs(true)
		.danger_accept_invalid_hostnames(true)
		.build()
		.context("Failed to build TLS connector")?;
	Ok(MakeTlsConnector::new(connector))
}

fn connect_database(config: &Config) -> Result<postgres::Client> {
	// Try SSL first (matching PostgreSQL's default sslmode=prefer behavior)
	match make_tls_connector() {
		Ok(tls) => match config.connect(tls) {
			Ok(client) => Ok(client),
			Err(_) => {
				// Fall back to non-SSL if SSL fails
				config.connect(postgres::NoTls).context("Failed to connect to database")
			}
		},
		Err(_) => {
			// If we can't create TLS connector, try non-SSL
			config.connect(postgres::NoTls).context("Failed to connect to database")
		}
	}
}

#[test]
fn test_create_timestamp() {
	assert_eq!(create_timestamp().len(), 14);
}

#[test]
#[serial_test::serial]
#[ignore]
fn test_ssl_connections() -> Result<()> {
	// Test 1: Non-SSL connection (sslmode=disable) - backward compatibility
	let non_ssl_url = std::env::var("PG_URL").unwrap_or_else(|_| 
		"postgres://experiment_user:asdf@localhost:5432/experiment-db?sslmode=disable".to_string()
	);
	let config: Config = non_ssl_url.parse()?;
	
	let client = connect_database(&config);
	assert!(client.is_ok(), "Non-SSL connection should succeed");
	
	// Test 2: Connection with sslmode=prefer (default PostgreSQL behavior)
	let prefer_url = non_ssl_url.replace("sslmode=disable", "sslmode=prefer");
	let config: Config = prefer_url.parse()?;
	let client = connect_database(&config);
	assert!(client.is_ok(), "Connection with sslmode=prefer should succeed");
	
	// Test 3: Verify TLS connector can be created
	let tls_connector = make_tls_connector();
	assert!(tls_connector.is_ok(), "TLS connector should be created successfully");
	
	Ok(())
}

#[test]
fn test_make_tls_connector() {
	let connector = make_tls_connector();
	assert!(connector.is_ok(), "Should be able to create TLS connector");
}

#[test]
fn test_connection_error_handling() {
	// Test with an invalid connection string that will fail both SSL and non-SSL
	let invalid_config: Config = "postgres://invalid:invalid@nonexistent:5432/invalid".parse().unwrap();
	let result = connect_database(&invalid_config);
	assert!(result.is_err(), "Should fail when both SSL and non-SSL connections fail");
	
	// Verify the error message indicates connection failure
	if let Err(e) = result {
		let error_msg = e.to_string();
		assert!(error_msg.contains("Failed to connect to database"), 
			"Error should indicate connection failure, got: {}", error_msg);
	}
}

fn get_null_string() -> String {
	"null".to_string()
}

fn ensure_directory(directory: &str) -> io::Result<()> {
	fs::create_dir_all(directory)
}

fn purge_directory(directory: &str) -> io::Result<()> {
	let directory = PathBuf::from(directory);
	match directory.exists() {
		true => fs::remove_dir_all(directory),
		false => Ok(()),
	}
}

const DEFAULT_MIGRATIONS_DIRECTORY: &'static str = "migrations";
const DEFAULT_SCHEMA_DIRECTORY: &'static str = "schema";

#[test]
#[serial_test::serial]
fn test_ensure_directory() -> io::Result<()> {
	purge_directory(DEFAULT_MIGRATIONS_DIRECTORY)?;
	ensure_directory(DEFAULT_MIGRATIONS_DIRECTORY)?;
	ensure_directory(DEFAULT_MIGRATIONS_DIRECTORY)?;
	purge_directory(DEFAULT_MIGRATIONS_DIRECTORY)?;
	purge_directory(DEFAULT_MIGRATIONS_DIRECTORY)?;
	ensure_directory(DEFAULT_MIGRATIONS_DIRECTORY)?;
	ensure_directory(DEFAULT_MIGRATIONS_DIRECTORY)?;
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

	for entry in WalkDir::new(directory) {
		let path = entry?.into_path();
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
	purge_directory(DEFAULT_MIGRATIONS_DIRECTORY)?;
	ensure_directory(DEFAULT_MIGRATIONS_DIRECTORY)?;

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

	purge_directory(DEFAULT_MIGRATIONS_DIRECTORY)?;
	Ok(())
}

#[test]
#[serial_test::serial]
fn test_list_sql_files_nested_schema() -> io::Result<()> {
	use pretty_assertions::assert_eq;

	purge_directory(&DEFAULT_SCHEMA_DIRECTORY)?;
	ensure_directory(&DEFAULT_SCHEMA_DIRECTORY)?;

	fs::File::create("schema/README")?;
	fs::File::create("schema/00_base.sql")?;
	fs::create_dir("schema/01_tables")?;
	fs::File::create("schema/01_tables/00_tables.sql")?;
	fs::create_dir("schema/01_tables/01_tables")?;
	fs::File::create("schema/01_tables/01_tables/README")?;
	fs::File::create("schema/01_tables/01_tables/00_tables.sql")?;
	fs::File::create("schema/01_tables/01_tables/01_tables.sql")?;
	fs::create_dir("schema/02_functions")?;
	fs::File::create("schema/02_functions/00_functions.sql")?;
	fs::create_dir("schema/02_functions/01_functions")?;
	fs::File::create("schema/02_functions/01_functions/00_functions.sql")?;
	fs::File::create("schema/02_functions/01_functions/01_functions.sql")?;
	fs::create_dir("schema/02_functions/02_functions")?;
	fs::File::create("schema/02_functions/02_functions/README")?;
	fs::File::create("schema/02_functions/02_functions/00_functions.sql")?;
	fs::File::create("schema/02_functions/02_functions/01_functions.sql")?;
	fs::File::create("schema/03_indexes.sql")?;

	let schema_files = list_sql_files(&DEFAULT_SCHEMA_DIRECTORY)?;
	assert_eq!(
		schema_files,
		vec![
			PathBuf::from("schema/00_base.sql"),
			PathBuf::from("schema/01_tables/00_tables.sql"),
			PathBuf::from("schema/01_tables/01_tables/00_tables.sql"),
			PathBuf::from("schema/01_tables/01_tables/01_tables.sql"),
			PathBuf::from("schema/02_functions/00_functions.sql"),
			PathBuf::from("schema/02_functions/01_functions/00_functions.sql"),
			PathBuf::from("schema/02_functions/01_functions/01_functions.sql"),
			PathBuf::from("schema/02_functions/02_functions/00_functions.sql"),
			PathBuf::from("schema/02_functions/02_functions/01_functions.sql"),
			PathBuf::from("schema/03_indexes.sql"),
		]
	);

	purge_directory(&DEFAULT_SCHEMA_DIRECTORY)?;
	Ok(())
}

#[derive(Debug, Eq, PartialEq)]
struct MigrationFile {
	file_path: PathBuf,
	display_file_path: String,
	current_version: String,
	previous_version: String,
	is_onboard: bool,
}

impl MigrationFile {
	/// file_paths is expected to be sorted alphanumerically
	fn vec_from_paths(file_paths: Vec<PathBuf>) -> Result<Vec<MigrationFile>> {
		let mut migration_files = vec![];
		let mut last_seen_current_version = get_null_string();

		for (index, file_path) in file_paths.into_iter().enumerate() {
			let display_file_path = file_path.to_string_lossy().to_string();

			// first parse the file_name and version strings
			let file_name = file_path.file_name().ok_or_else(|| anyhow!("no file name for this path: {display_file_path}"))?;
			let file_name = file_name.to_str().ok_or_else(|| anyhow!("file name isn't valid unicode: {display_file_path}"))?;
			let mut portions = file_name.split(".");
			let current_version = portions.next()
				.ok_or_else(|| anyhow!("no version strings in this path: {display_file_path}"))?.to_string();
			let previous_version = portions.next()
				.ok_or_else(|| anyhow!("no previous version string in this path: {display_file_path}"))?.to_string();

			// then check that the version strings align with the previous one
			if previous_version == "onboard" && last_seen_current_version == "null" {
				last_seen_current_version = "onboard".to_string()
			}
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
			let is_onboard = previous_version == "onboard";
			let previous_version = match previous_version == "null" || is_onboard {
				true => {
					// check that nulls are only allowed in the first spot
					if !(index == 0) {
						return Err(anyhow!("null or onboard previous_version in migration that isn't the first: {display_file_path}"));
					}
					get_null_string()
				},
				false => {
					let previous_version = previous_version;
					if !(current_version > previous_version) {
						return Err(anyhow!("all migration versions have to be sequential, so {current_version} must be greater than {previous_version}"));
					}
					validate_version_string(previous_version)?
				}
			};

			migration_files.push(MigrationFile{file_path, display_file_path, current_version, previous_version, is_onboard});
		}

		Ok(migration_files)
	}
}

#[test]
fn test_migration_files_vec_from_paths() {
	let ex = |file_path: PathBuf, current_version: &str, previous_version: &str| {
		let display_file_path = file_path.to_string_lossy().to_string();
		let is_onboard = previous_version == "onboard";
		MigrationFile{
			file_path, display_file_path,
			current_version: current_version.to_string(),
			previous_version: if is_onboard { get_null_string() } else { previous_version.to_string() },
			is_onboard,
		}
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
	let file_path = PathBuf::from(format!("ok/{version}.onboard.sql"));
	assert_eq!(
		MigrationFile::vec_from_paths(vec![file_path.clone()]).unwrap(),
		vec![ex(file_path, &version, "onboard")],
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

	let file_path1 = PathBuf::from(format!("ok/{version}.onboard.sql"));
	let file_path2 = PathBuf::from(format!("ok/90000000000000.{version}.sql"));
	let file_path3 = PathBuf::from(format!("ok/90000000000001.90000000000000.sql"));
	let file_path4 = PathBuf::from(format!("ok/90000000000002.90000000000001.sql"));
	assert_eq!(
		MigrationFile::vec_from_paths(vec![file_path1.clone(), file_path2.clone(), file_path3.clone(), file_path4.clone()]).unwrap(),
		vec![
			ex(file_path1, &version, "onboard"),
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


fn gather_validated_migrations(args: &Args) -> Result<(Vec<MigrationFile>, Option<String>)> {
	// TODO use client to grab existing migrations and check them against the directory?

	ensure_directory(&args.migrations_directory)?;
	let migration_files = MigrationFile::vec_from_paths(list_sql_files(&args.migrations_directory)?)?;

	let current_version = migration_files.last().map(|migration_file| migration_file.current_version.clone());

	Ok((migration_files, current_version))
}


#[derive(Debug)]
enum SchemaArg {
	OnlySchema(String),
	ExcludeSchema(String),
}

fn compute_diff(source: &Config, target: &Config, exclude_privileges: bool, schema_arg: &Option<SchemaArg>) -> Result<String> {
	let mut cmd = std::process::Command::new("migra");
	cmd.arg("--unsafe");

	if !exclude_privileges {
		cmd.arg("--with-privileges");
	}
	match schema_arg {
		None => {},
		Some(SchemaArg::OnlySchema(schema)) => { cmd.arg("--schema").arg(schema); },
		Some(SchemaArg::ExcludeSchema(exclude_schema)) => { cmd.arg("--exclude_schema").arg(exclude_schema); },
	};

	let output = cmd
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
	let mut client = connect_database(config)?;
	for sql_file in sql_files {
		let mut file = fs::File::open(sql_file)?;
		let mut query = String::new();
		file.read_to_string(&mut query)?;
		client.batch_execute(&query)?;
	}

	Ok(())
}


fn command_generate(args: &Args, raw_description: &str, is_onboard: bool) -> Result<String> {
	let dbname = args.pg_url.get_dbname().ok_or_else(|| anyhow!("need a dbname to run generate command"))?;
	let (migration_files, previous_version) = gather_validated_migrations(&args)?;
	if is_onboard && previous_version.is_some() {
		return Err(anyhow!("can't generate an onboard migration when there are already migrations"));
	}
	let previous_version = previous_version.unwrap_or_else(|| if is_onboard { "onboard".to_string() } else { get_null_string() });

	let description_slug = make_slug(raw_description);
	let current_version = create_timestamp();

	let source = TempDb::new(&dbname, "migrations", &args.pg_url)?;
	apply_sql_files(&source.config, migration_files.into_iter().map(|migration_file| migration_file.file_path).collect())?;
	let target = TempDb::new(&dbname, "schema", &args.pg_url)?;
	apply_sql_files(&target.config, list_sql_files(&args.schema_directory)?)?;

	let generated_migration = compute_diff(&source.config, &target.config, args.exclude_privileges, &args.schema_arg)?;

	fs::File::create(format!("./{}/{current_version}.{previous_version}.{description_slug}.sql", args.migrations_directory))?
		.write_all(generated_migration.as_bytes())?;

	Ok(current_version)
}


fn command_compact(args: &Args) -> Result<()> {
	let mut client = connect_database(&args.pg_url)?;
	command_generate(args, "ensuring_current", false)?;
	command_migrate(args, &mut client, false, false)?;

	purge_directory(&args.migrations_directory)?;
	ensure_directory(&args.migrations_directory)?;
	let current_version = command_generate(args, "compacted_initial", false)?;
	println!("new version number is: {current_version}");

	client.batch_execute(&format!("
		truncate table _schema_versions;
		insert into _schema_versions (current_version, previous_version) values ({current_version}, null)
	"))?;
	Ok(())
}

const EXISTS_QUERY: &'static str = "select true from pg_catalog.pg_class where relname = '_schema_versions' and relkind = 'r'";

fn command_migrate(
	args: &Args, client: &mut postgres::Client,
	actually_perform_onboard_migrations: bool,
	dry_run: bool,
) -> Result<()> {
	let migration_files = gather_validated_migrations(&args)?.0;

	let actual_version: Option<String> = {
		let mut transaction = client.transaction()?;
		transaction.execute(&format!(r#"
			create function pg_temp.current_schema_version() returns setof char(14) as $$
			begin
				if ({EXISTS_QUERY}) then
					return query select max(current_version) from _schema_versions;
				else
					return query select null::char(14);
				end if;
			end;
			$$ language plpgsql;
		"#), &[])?;

		transaction
			.query_one("select pg_temp.current_schema_version() as current_version", &[])?
			.get("current_version")
	};

	let performing_prefix = if dry_run { "would perform" } else { "performing" };

	for (index, MigrationFile{display_file_path, file_path, current_version, previous_version, is_onboard}) in migration_files.iter().enumerate() {
		let is_onboard = *is_onboard;
		if index != 0 && is_onboard {
			return Err(anyhow!("migration {display_file_path} is listed as an onboard migration, but isn't the first one (at index {index})"));
		}

		let mut perform_migration = || -> Result<()> {
			if dry_run { return Ok(()) }

			if index == 0 {
				create_versions_table(client)?;
			}

			let mut transaction = client.transaction()?;

			if !is_onboard || actually_perform_onboard_migrations {
				let mut file = fs::File::open(&file_path)?;
				let mut migration_query = String::new();
				file.read_to_string(&mut migration_query)?;
				transaction.batch_execute(&migration_query)?;
			}

			transaction.batch_execute(&format!("
				insert into _schema_versions (current_version, previous_version) values ({current_version}, {previous_version})
			"))?;
			transaction.commit()?;

			Ok(())
		};

		match actual_version {
			None => {
				println!("{performing_prefix} {}", display_file_path);
				perform_migration()?
			},
			Some(ref actual_version) => {
				if current_version > actual_version {
					println!("{performing_prefix} {}", display_file_path);
					perform_migration()?;
				}
				else {
					println!("not {performing_prefix} {}", display_file_path);
				}
			},
		}
	}

	Ok(())
}

fn command_clean(mut base_config: Config) -> Result<()> {
	let mut client = connect_database(&base_config.dbname("template1"))?;
	let query = format!("
		select databases.datname as dbname
		from
			pg_database as databases
			join pg_shdescription as descriptions on descriptions.objoid = databases.oid
		where descriptions.description = {TEMP_DB_COMMENT}
	");
	for row in client.query(&query, &[])? {
		let dbname: String = row.get("dbname");
		client.batch_execute(&format!(r#"drop database if exists "{dbname}""#))?;
	}

	Ok(())
}


fn create_versions_table(client: &mut postgres::Client) -> Result<()> {
	client.batch_execute("
		create table _schema_versions (
			current_version char(14) not null unique,
			previous_version char(14) references _schema_versions(current_version) unique,
			check (current_version > previous_version)
		);
		create unique index if not exists i_schema_versions on _schema_versions ((previous_version is null)) where previous_version is null
	")?;

	Ok(())
}

fn ensure_db(args: &Args, dbname: &str, base_config: &Config, backend: Backend, need_version_table: bool) -> Result<(Option<TempDb>, Config)> {
	let do_it = |suffix: &'static str, dir: &str| {
		let temp = TempDb::new(dbname, suffix, base_config)?;
		if need_version_table {
			let mut client = connect_database(&temp.config)?;
			create_versions_table(&mut client)?;
		}
		apply_sql_files(&temp.config, list_sql_files(dir)?)?;

		let config = temp.config.clone();
		Ok((Some(temp), config))
	};

	match backend {
		Backend::Migrations => { do_it("migrations", &args.migrations_directory) },
		Backend::Schema => { do_it("schema", &args.schema_directory) },
		Backend::Database => Ok((None, base_config.clone())),
	}
}

fn compute_backend_diff(args: &Args, source: Backend, target: Backend) -> Result<String> {
	// TODO we could implement ignores by asking for sql that we just apply to other sources before we diff them against the database

	if source == target {
		return Err(anyhow!("can't diff {:?} against itself", source))
	}

	let need_version_table: bool = match (source, target) {
		(_, Backend::Database) | (Backend::Database, _) => {
			let mut client = connect_database(&args.pg_url)?;
			client.query_one(&format!("select exists ({EXISTS_QUERY}) as table_exists"), &[])?.get("table_exists")
		},
		_ => false,
	};

	let dbname = args.pg_url.get_dbname().ok_or(anyhow!("provided pg_url has no dbname"))?;
	let source = ensure_db(args, dbname, &args.pg_url, source, need_version_table)?;
	let target = ensure_db(args, dbname, &args.pg_url, target, need_version_table)?;
	Ok(compute_diff(&source.1, &target.1, args.exclude_privileges, &args.schema_arg)?)
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


const TEMP_DB_COMMENT: &'static str = "'TEMP DB CREATED BY postgres_migrator'";

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

		let mut client = connect_database(&base_config.clone().dbname("template1"))?;
		client.execute(&format!(r#"create database "{dbname}""#), &[])?;
		client.batch_execute(&format!(r#"comment on database "{dbname}" is {TEMP_DB_COMMENT}"#))?;

		Ok(TempDb{dbname, config})
	}
}

impl Drop for TempDb {
	fn drop(&mut self) {
		let dbname = &self.dbname;

		let _ = connect_database(&self.config.dbname("template1"))
			.map_err(|err| { eprintln!("unable to drop {dbname}: {err}"); err })
			.and_then(|mut client| {
				client.batch_execute(&format!(r#"drop database if exists "{dbname}""#))
					.map_err(|e| anyhow::anyhow!(e))
			})
			.map_err(|err| { eprintln!("unable to drop {dbname}: {err}"); err });
	}
}


#[derive(Parser, Debug)]
#[clap(author, version)]
struct RawArgs {
	/// postgres connection string, in the form postgres://user:password@host:port/database
	/// can also be loaded from the environment variable PG_URL
	#[clap(long, env = "PG_URL", parse(try_from_str = config_try_from_str))]
	pg_url: Config,

	/// opposite of migra [`--with-privileges`](https://github.com/djrobstep/migra/blob/master/docs/options.md#--with-privileges)
	#[clap(long)]
	exclude_privileges: bool,

	/// pass-through of migra [`--schema [SCHEMA_NAME]`](https://github.com/djrobstep/migra/blob/master/docs/options.md#--schema-schema_name)
	#[clap(long)]
	schema: Option<String>,

	/// pass-through of migra [`--exclude_schema [SCHEMA_NAME]`](https://github.com/djrobstep/migra/blob/master/docs/options.md#--exclude_schema-schema_name)
	#[clap(long)]
	exclude_schema: Option<String>,

	// #[clap(flatten)]
	// schema_arg: Option<SchemaArg>,

	/// directory where the declarative schema is located
	#[clap(long, default_value_t = String::from(DEFAULT_SCHEMA_DIRECTORY))]
	schema_directory: String,
	/// directory where migrations are stored
	#[clap(long, default_value_t = String::from(DEFAULT_MIGRATIONS_DIRECTORY))]
	migrations_directory: String,

	#[clap(subcommand)]
	command: Command,
}

#[derive(Debug)]
struct Args {
	pg_url: Config,
	exclude_privileges: bool,
	schema_arg: Option<SchemaArg>,
	schema_directory: String,
	migrations_directory: String,
	command: Command,
}

impl Args {
	fn from_raw_args(raw_args: RawArgs) -> Result<Args> {
		let RawArgs{pg_url, exclude_privileges, schema, exclude_schema, schema_directory, migrations_directory, command} = raw_args;

		let schema_arg = match (schema, exclude_schema) {
			(Some(schema), Some(exclude_schema)) => {
				return Err(anyhow!("can't set both schema and exclude-schema (schema={schema}, exclude-schema={exclude_schema})"));
			},
			(Some(schema), None) => Some(SchemaArg::OnlySchema(schema)),
			(None, Some(exclude_schema)) => Some(SchemaArg::ExcludeSchema(exclude_schema)),
			(None, None) => None,
		};

		Ok(Args {
			pg_url, exclude_privileges,
			schema_directory, migrations_directory,
			schema_arg,
			command,
		})
	}
}

#[derive(clap::Subcommand, Debug)]
enum Command {
	/// generate new migration and place in migrations folder
	Generate {
		/// description of migration, will be converted to "snake_case"
		migration_description: String,
		/// generate an "onboarding" migration,
		/// to get postgres_migrator attached to a database that already has a schema
		#[clap(long)]
		is_onboard: bool,
	},
	/// apply all migrations to database
	Migrate {
		/// necessary in dev situations where a clean database needs to have all migrations performed
		#[clap(long)]
		actually_perform_onboard_migrations: bool,

		#[clap(long)]
		dry_run: bool,
	},
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
	let args = Args::from_raw_args(RawArgs::parse())?;

	match args.command {
		Command::Generate{ref migration_description, is_onboard} => {
			command_generate(&args, &migration_description, is_onboard)?;
		},
		Command::Migrate {actually_perform_onboard_migrations, dry_run} => {
			let mut client = connect_database(&args.pg_url)?;
			command_migrate(&args, &mut client, actually_perform_onboard_migrations, dry_run)?;
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
fn test_full_no_onboard() -> Result<()> {
	fn get_config() -> Config {
		std::env::var("PG_URL").unwrap().parse::<Config>().unwrap()
	}
	fn get_args(schema_directory: &'static str) -> Args {
		Args {
			pg_url: get_config(),
			schema_directory: schema_directory.to_string(),
			migrations_directory: DEFAULT_MIGRATIONS_DIRECTORY.to_string(),
			command: Command::Clean,
			exclude_privileges: false,
			schema_arg: None,
		}
	}

	fn get_migration_count() -> usize {
		list_sql_files(DEFAULT_MIGRATIONS_DIRECTORY).unwrap().len()
	}

	let mut client = connect_database(&get_config())?;
	client.batch_execute("
		drop schema public cascade;
		create schema public;
		grant all on schema public to public;
		comment on schema public is 'standard public schema';
	")?;
	purge_directory(DEFAULT_MIGRATIONS_DIRECTORY)?;
	ensure_directory(DEFAULT_MIGRATIONS_DIRECTORY)?;

	use Backend::*;
	assert!(command_check(&get_args("schemas/schema.1"), Database, Migrations).is_ok());
	assert!(command_check(&get_args("schemas/schema.1"), Schema, Migrations).is_err());
	assert!(command_check(&get_args("schemas/schema.1"), Database, Schema).is_err());
	assert!(!compute_backend_diff(&get_args("schemas/schema.1"), Database, Schema)?.is_empty());
	assert!(compute_backend_diff(&get_args("schemas/schema.1"), Database, Migrations)?.is_empty());

	// # schema.1
	command_generate(&get_args("schemas/schema.1"), "one", false)?;
	assert_eq!(get_migration_count(), 1);
	let migration = &gather_validated_migrations(&get_args(""))?.0[0];
	assert!(!migration.is_onboard);
	assert!(migration.previous_version == get_null_string());
	command_migrate(&get_args(""), &mut connect_database(&get_config())?, false, false)?;
	client.batch_execute("select id, name, color from fruit")?;

	// # schema.2
	command_generate(&get_args("schemas/schema.2"), "two", false)?;
	assert_eq!(get_migration_count(), 2);
	command_migrate(&get_args(""), &mut connect_database(&get_config())?, false, false)?;
	client.batch_execute("select id, name, flavor from fruit")?;

	// # schema.3
	command_compact(&get_args("schemas/schema.3"))?;
	assert_eq!(get_migration_count(), 1);
	client.batch_execute("select person.name, fruit.name, flavor from person join fruit on person.favorite_fruit = fruit.id where flavor = 'SALTY'")?;

	// # schema.1
	command_generate(&get_args("schemas/schema.1"), "back to one", false)?;
	assert_eq!(get_migration_count(), 2);
	command_migrate(&get_args(""), &mut connect_database(&get_config())?, false, false)?;
	client.batch_execute("select id, name, color from fruit")?;

	command_clean(get_config())?;
	client.execute("create database garbage_tmp", &[])?;
	client.batch_execute("comment on database garbage_tmp is 'TEMP DB CREATED BY postgres_migrator';")?;
	command_clean(get_config())?;
	// this is just a ghetto way to make sure `clean` actually removes garbage_tmp, since this command will fail otherwise
	client.execute("create database garbage_tmp", &[])?;
	client.execute("drop database garbage_tmp", &[])?;

	Ok(())
}

#[test]
#[serial_test::serial]
#[ignore]
fn test_full_with_onboard() -> Result<()> {
	fn get_config() -> Config {
		std::env::var("PG_URL").unwrap().parse::<Config>().unwrap()
	}
	fn get_args(schema_directory: &'static str) -> Args {
		Args {
			pg_url: get_config(),
			schema_directory: schema_directory.to_string(),
			migrations_directory: DEFAULT_MIGRATIONS_DIRECTORY.to_string(),
			command: Command::Clean,
			exclude_privileges: false,
			schema_arg: None,
		}
	}

	fn get_migration_count() -> usize {
		list_sql_files(DEFAULT_MIGRATIONS_DIRECTORY).unwrap().len()
	}

	let mut client = connect_database(&get_config())?;
	client.batch_execute("
		drop schema public cascade;
		create schema public;
		grant all on schema public to public;
		comment on schema public is 'standard public schema';
	")?;
	purge_directory(DEFAULT_MIGRATIONS_DIRECTORY)?;
	ensure_directory(DEFAULT_MIGRATIONS_DIRECTORY)?;

	use Backend::*;
	assert!(command_check(&get_args("schemas/schema.1"), Database, Migrations).is_ok());
	assert!(command_check(&get_args("schemas/schema.1"), Schema, Migrations).is_err());
	assert!(command_check(&get_args("schemas/schema.1"), Database, Schema).is_err());
	assert!(!compute_backend_diff(&get_args("schemas/schema.1"), Database, Schema)?.is_empty());
	assert!(compute_backend_diff(&get_args("schemas/schema.1"), Database, Migrations)?.is_empty());

	// # schema.1
	// generate one using some schema
	command_generate(&get_args("schemas/schema.1"), "one", true)?;
	assert_eq!(get_migration_count(), 1);
	let migration = &gather_validated_migrations(&get_args(""))?.0[0];
	assert!(migration.is_onboard);
	assert!(migration.previous_version == get_null_string());
	// manually apply the schema
	apply_sql_files(&get_config(), vec![PathBuf::from("schemas/schema.1/schema.sql")])?;
	// apply migrations, which should work
	command_migrate(&get_args(""), &mut connect_database(&get_config())?, false, false)?;
	client.batch_execute("select id, name, color from fruit")?;
	// check diff is clean
	assert!(command_check(&get_args("schemas/schema.1"), Database, Migrations).is_ok());
	assert!(command_check(&get_args("schemas/schema.1"), Database, Schema).is_ok());
	assert!(command_check(&get_args("schemas/schema.1"), Schema, Migrations).is_ok());

	// everthing else we do should continue to work
	// # schema.2
	command_generate(&get_args("schemas/schema.2"), "two", false)?;
	assert_eq!(get_migration_count(), 2);
	command_migrate(&get_args(""), &mut connect_database(&get_config())?, false, false)?;
	client.batch_execute("select id, name, flavor from fruit")?;

	// # schema.3
	command_compact(&get_args("schemas/schema.3"))?;
	assert_eq!(get_migration_count(), 1);
	client.batch_execute("select person.name, fruit.name, flavor from person join fruit on person.favorite_fruit = fruit.id where flavor = 'SALTY'")?;

	// # schema.1
	command_generate(&get_args("schemas/schema.1"), "back to one", false)?;
	assert_eq!(get_migration_count(), 2);
	command_migrate(&get_args(""), &mut connect_database(&get_config())?, false, false)?;
	client.batch_execute("select id, name, color from fruit")?;

	command_clean(get_config())?;
	client.execute("create database garbage_tmp", &[])?;
	client.batch_execute("comment on database garbage_tmp is 'TEMP DB CREATED BY postgres_migrator';")?;
	command_clean(get_config())?;
	// this is just a ghetto way to make sure `clean` actually removes garbage_tmp, since this command will fail otherwise
	client.execute("create database garbage_tmp", &[])?;
	client.execute("drop database garbage_tmp", &[])?;

	Ok(())
}
