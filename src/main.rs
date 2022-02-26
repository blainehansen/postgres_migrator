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


fn db_migrate(client: &mut postgres::Client, raw_description: &str) -> SomeResult<()> {
	let description_slug = make_slug(raw_description);
	let timestamp = create_timestamp();

	let migration_up = "TODO";
	// print('\n'.join([
	// 	"creating migration file:",
	// 	"===",
	// 	migration_up,
	// 	"===",
	// ]))

	ensure_migrations_directory()?;
	fs::File::create(format!("./migrations/{timestamp}.{description_slug}.sql"))?
		.write_all(migration_up.as_bytes())?;

	Ok(())
}


fn db_compact(client: &mut postgres::Client) -> SomeResult<()> {
	db_migrate(client, "ensuring_current")?;
	db_up(client)?;

	purge_migrations_directory()?;
	ensure_migrations_directory()?;
	db_migrate(client, "compacted_initial")?;
	let migration_files = list_migration_files()?;
	let version = migration_files[0].to_str().unwrap().split('.').nth(0).unwrap();
	println!("new version number is: {version}", );

	client.batch_execute(&format!("truncate table _schema_versions; insert into _schema_versions (version) values ({version})"))?;
	Ok(())
}


fn db_up(client: &mut postgres::Client) -> SomeResult<()> {
	client.execute("create table if not exists _schema_versions (version char(14) unique not null)", &[])?;

	let current_version: Option<String> = client
		.query_opt("select max(version) as current from _schema_versions", &[])?
		.map(|row| row.get("current"));
	// println!("current version is: {current_version}");

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


fn main() -> SomeResult<()> {
	let mut client = postgres::Config::new()
		.user("experiment_user")
		.password("asdf")
		.host("localhost")
		.port(5432)
		.dbname("experiment_db")
		.ssl_mode(postgres::config::SslMode::Disable)
		.connect(postgres::NoTls)?;

	db_compact(&mut client)?;
	db_migrate(&mut client, "yo yo")?;
	db_up(&mut client)?;

	Ok(())
}
