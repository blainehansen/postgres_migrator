# `postgres_migrator`

`postgres_migrator` allows you to write your postgres schema directly in *declarative sql*, to automatically generate migrations when you change that declarative schema, and to apply those migrations with rigorous version tracking and consistency checks.

**No more orms!** Use the full power of postgres directly without having to manually write migrations.

`postgres_migrator` is able to:

- Automatically generate raw sql migrations by diffing the sql in a migrations folder to that in a schema folder.
- Apply those migrations and save migration version numbers in the database, so you can know what state a database is supposed to be in.
- Run consistency checks on migrations, so that they are only ever applied in the order they were generated. This can prevent very painful debugging and database corruption.

`postgres_migrator` intentionally doesn't do the following:

- Create "down" versions of migrations. If you want to undo something in production, just make a new migration (that's best practice anyway). In dev just force the database into the right state.
- Allow running migrations only up to a certain version. `postgres_migrator` will always apply all available unapplied migrations. If you don't want to apply some migrations, move them to a different folder or change their extension to something other than `.sql`.
- Figure out database diffs itself, but instead uses the well-establised [`migra`](https://github.com/djrobstep/migra) under the hood.

# Example

If your `schema` directory contains a sql file like this:

```sql
create table fruit (
  id serial primary key,
  name text not null unique,
  color text not null default ''
);
```

Then running `postgres_migrator generate 'add fruit table'` will generate a migration called `$new_version.$previous_version.add_fruit_table.sql` in the `migrations` folder.

You can then run `postgres_migrator migrate` to run this migration (and any others that haven't been run).

If you then change your schema sql to this:

```sql
create type flavor_type as enum('SWEET', 'SAVORY');

create table fruit (
  id serial primary key,
  name text not null unique,
  flavor flavor_type not null default 'SWEET'
);
```

Then running `postgres_migrator generate 'remove color add flavor'` will generate `$new_version.$previous_version.remove_color_add_flavor.sql` that will go from the previous state to the new state.

# Usage

First, place your declarative sql files in the `schema` directory and create a directory for migrations called `migrations`. You can customize these with `--schema-directory` and `--migrations-directory`.

You can put any valid sql in the `migrations` files, you don't have to accept the automatically generated sql as is. All this tool cares about is the difference between the final sql objects created by sql in the `schema` and `migrations` directories, [and however you achieve that is up to you!](https://github.com/blainehansen/postgres_migrator/issues/4)

## As a standalone docker image

`postgres_migrator` is distributed as a docker image, `blainehansen/postgres_migrator`. You can run it using `docker run`, and since the cli needs to interact with a postgres database, read schema files, and read/write migration files, it needs quite a few options:


```bash
docker run --rm -it --network host -u $(id -u ${USER}):$(id -g ${USER}) -v $(pwd):/working blainehansen/postgres_migrator <args>
```

To make this easier to manage, you can package that command in a function, alias, or script:

```bash
function postgres_migrator {
  local result=$(docker run --rm -it --network host -u $(id -u ${USER}):$(id -g ${USER}) -v $(pwd):/working -e PG_URL=$PG_URL blainehansen/postgres_migrator "$@")
  echo $result
  return $?
}

# or
alias postgres_migrator="docker run --rm -it --network host -u $(id -u ${USER}):$(id -g ${USER}) -v $(pwd):/working -e PG_URL=$PG_URL blainehansen/postgres_migrator"

# or in it's own executable file
docker run --rm -it --network host -u $(id -u ${USER}):$(id -g ${USER}) -v $(pwd):/working -e PG_URL=$PG_URL blainehansen/postgres_migrator "$@"

# now you can call it more cleanly
postgres_migrator generate 'adding users table'
postgres_migrator migrate
```

## With docker compose

Many people run postgres as one service in a docker compose setup, either using a `postgres` image or something like `cloud-sql-proxy`.

```yml
version: '3'
services:
  postgres_migrator:
    image: blainehansen/postgres_migrator
    container_name: postgres_migrator
    depends_on:
      - db
    environment:
      - PG_URL=postgres://experiment_user:asdf@db:5432/experiment_db?sslmode=disable
    volumes:
      - .:/working/
    tty: true
    entrypoint: tail -F anything

  db:
    image: postgres:alpine
    container_name: db
    environment:
      - POSTGRES_DB=experiment_db
      - POSTGRES_USER=experiment_user
      - POSTGRES_PASSWORD=asdf
    ports:
      - "5432:5432"
    command: postgres -c 'max_wal_size=2GB'
```

If that is running, a command like this should work:

```sh
docker exec -it -u $(id -u ${USER}):$(id -g ${USER}) postgres_migrator postgres_migrator migrate
```

## As a Rust binary

This package is published to [crates.io](https://crates.io/crates/postgres_migrator), so you can use `cargo install postgres_migrator` to install it.

The package calls the [`migra`](https://github.com/djrobstep/migra) command, so that must be installed and runnable.

---

Cli usage:

```
USAGE:
    postgres_migrator [OPTIONS] --pg-url <PG_URL> <SUBCOMMAND>

OPTIONS:
    -h, --help
            Print help information

        --pg-url <PG_URL>
            postgres connection string, in the form postgres://user:password@host:port/database
            can also be loaded from the environment variable PG_URL [env: PG_URL=]

        --migrations-directory <MIGRATIONS_DIRECTORY>
            directory where migrations are stored [default: migrations]

        --schema-directory <SCHEMA_DIRECTORY>
            directory where the declarative schema is located [default: schema]

    -V, --version
            Print version information

SUBCOMMANDS:
    generate    generate new migration and place in migrations folder
    migrate     apply all migrations to database
    check       checks that `source` and `target` are in sync, throws error otherwise
    diff        prints out the sql diff necessary to convert `source` to `target`
    compact     ensure both database and migrations folder are current with schema and compact
                to only one migration
    clean       cleans the current instance of all temporary databases
    help        Print this message or the help of the given subcommand(s)
```

## How to use with an existing database?

If you already have a database with an existing schema, you need to generate your first migration using the `--is-onboard` flag:

```bash
postgres_migrator generate 'first migration with postgres_migrator' --is-onboard
# migrations folder should have migration named `$new_version.onboard.first_migration_with_postgres_migrator.sql`
```

The `--is-onboard` flag changes the first migration to be an "onboarding" migration. When this migration is run, the actual sql in the migration won't be applied, and instead `postgres_migrator` will just create the `_schema_versions` table and insert the version of the migration.

After you've created this first "onboarding" migration, and can just use `postgres_migrator` as usual!

## What is `compact`?

Over time a migrations folder can get large and unwieldy, with possibly hundreds of migrations. This long log gets less and less useful over time, especially for small teams. The `compact` command replaces all migrations with a single migration that creates the entire schema at once.

Some teams will consider this dangerous and unnecessary, and they're free to not use it!

# Credits

- [`migra`](https://github.com/djrobstep/migra) for making it possible to diff schemas.
- [`tusker`](https://github.com/bikeshedder/tusker) was the inspiration for using temporary databases as diff targets. `postgres_migrator` adds the ability to generate and run versioned migrations and to perform compaction.
- Thank you [Rust](https://www.rust-lang.org/) for being so awesome! [clap](https://github.com/clap-rs/clap) and [rust-postgres](https://github.com/sfackler/rust-postgres) in particular made this way easier.

# Contributing

Pull requests to make the script more ergonomic or robust are welcome.
