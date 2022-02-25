use std::{io, fs, path::PathBuf};
use chrono::{Utc};


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

#[test]
fn test_ensure_migrations_directory() -> io::Result<()> {
	ensure_migrations_directory()?;
	ensure_migrations_directory()?;
	Ok(())
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
fn test_list_migration_files() -> io::Result<()> {
	fs::remove_dir_all("./migrations")?;
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

	fs::remove_dir_all("./migrations")?;
	Ok(())
}


fn main() {
	let timestamp_string = create_timestamp();
	println!("{timestamp_string}");
}
