import os
import re
import sys
import migra
import psycopg2
from datetime import datetime

def show_help():
	message = '\n'.join([
		f"Usage: <subcommand>",
		"commands:",
		"    migrate <migration description>",
		"    up",
		"    compact",
		"",
	])
	print(message)

def show_help_error():
	show_help()
	sys.exit(1)


def create_timestamp():
	return datetime.now().strftime(r"%Y%m%d%H%M%S")

def ensure_migrations_directory():
	os.makedirs('./migrations', exist_ok=True)

def list_migration_files():
	return sorted(os.listdir('./migrations'))

def db_compact():
	db_migrate('ensuring_current')
	db_up()

	# TODO account for situation where migrations doesn't exist
	os.rmdir('./migrations')
	ensure_migrations_directory()
	db_migrate('compacted_initial')
	migration_files = list_migration_files()
	version_number = migration_files[0].split('.')[0]
	print(f'new version number is: {version_number}')

	cur.execute(f'truncate table _schema_versions; insert into _schema_versions (version_number) values ({version_number})')

def db_migrate(raw_description):
	if not raw_description:
		show_help_error()

	description_slug = re.sub(r'\s+', '_', raw_description)
	print(description_slug)
	timestamp = create_timestamp()

	migration_up = 'TODO'
	print('\n'.join([
		"creating migration file:",
		"===",
		migration_up,
		"===",
	]))

	ensure_migrations_directory()
	with open(f'./migrations/{timestamp}.{description_slug}.sql', 'w') as output_file:
		output_file.write(migration_up)

def db_up():
	cur.execute('CREATE table if not exists _schema_versions (version_number char(14) unique not null)')
	conn.commit()

	# grab the latest version
	current_version_number, = cur.execute('SELECT max(version_number) from _schema_versions').fetchall()
	print(f'current version is: {current_version_number}')

	ensure_migrations_directory()
	for migration_file in list_migration_files():
		version_number = migration_file.split('.')[0]

		if not current_version_number or version_number > current_version_number:
			print(f'performing {migration_file}')
			migration_query = None
			with open(f'./migrations/{migration_file}') as query_file:
				migration_query = query_file.read()
			cur.execute(f'{migration_query}; INSERT into _schema_versions (version_number) values ({version_number})')
		else:
			print(f'not performing {migration_file}')


import click

@click.command()
def main():
	# directory = "migrations"
	# filename = "schemas/schema.1.sql"
	conn = psycopg2.connect('postgres://experiment_user:asdf@localhost:5432/experiment_db?sslmode=disable')
	cur = conn.cursor()

	migration = migra.Migration(
		source,
		target,
		# self.config.database.schema
	)
	migration.set_safety(False)
	migration.add_all_changes(privileges=with_privileges)
	return migration.sql


if __name__ == '__main__':
	main()
