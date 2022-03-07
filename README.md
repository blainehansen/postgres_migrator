# `migrator`

`migrator` allows you to write your postgres schema directly in *declarative sql*, to automatically generate migrations when you change that declarative schema, and to apply those migrations with rigorous version tracking and consistency checks.

**No more orms!** Use the full power of postgres directly without having to manually write migrations.

`migrator` is able to:

- Automatically generate raw sql migrations by diffing the sql in a migrations folder to that in a schema folder.
- Apply those migrations and save migration version numbers in the database, so you can know what state a database is supposed to be in.
- Run consistency checks on migrations, so that they are only ever applied in the order they were generated. This can prevent very painful debugging and database corruption.

`migrator` intentionally doesn't do the following:

- Create "down" versions of migrations. If you want to undo something in production, just make a new migration (that's best practice anyway). In dev just force the database into the right state.
- Allow running migrations only up to a certain version, and instead always applies all migrations. If you don't want to apply some migrations, move them to a different folder or change their extension to something other than `.sql`.
- Figure out database diffs itself, instead using the well-establised [`migra`](https://github.com/djrobstep/migra) under the hood.

# Usage

First, place your declarative sql files in the `schema` directory and create a directory for migrations called `migrations`. You can customize these with `--schema-directory` and `--migrations-directory`.

`migrator` is a cli that is distributed as a docker image, `blainehansen/migrator`. You can run it using `docker run`, and since the cli needs to interact with a postgres database, read schema files, and read/write migration files, it needs quite a few options:


```bash
docker run --rm -it --network host -u $(id -u ${USER}):$(id -g ${USER}) -v $(pwd):/working blainehansen/migrator <args>
```

To make this easier to manage, you can package that command in a function, alias, or script:

```bash
function migrator {
  local result=$(docker run --rm -it --network host -u $(id -u ${USER}):$(id -g ${USER}) -v -e PG_URL=$PG_URL $(pwd):/working blainehansen/migrator "$@")
  echo $result
  return $?
}

# or
alias migrator="docker run --rm -it --network host -u $(id -u ${USER}):$(id -g ${USER}) -v -e PG_URL=$PG_URL $(pwd):/working blainehansen/migrator"

# or in it's own executable file
docker run --rm -it --network host -u $(id -u ${USER}):$(id -g ${USER}) -v -e PG_URL=$PG_URL $(pwd):/working blainehansen/migrator "$@"

# now you can call it more cleanly
migrator migrate 'adding users table'
migrator up
```

Here's the cli usage:

```
USAGE:
    migrator [OPTIONS] --pg-url <PG_URL> <SUBCOMMAND>

OPTIONS:
    -h, --help
            Print help information

    -V, --version
            Print version information

        --pg-url <PG_URL>
            postgres connection string, in the form postgres://user:password@host:port/database
            can also be loaded from the environment variable PG_URL [env: PG_URL=]

        --migrations-directory <MIGRATIONS_DIRECTORY>
            directory where migrations are stored [default: migrations]

        --schema-directory <SCHEMA_DIRECTORY>
            directory where the declarative schema is located [default: schema]

SUBCOMMANDS:
    help       Print this message or the help of the given subcommand(s)
    up         apply all migrations to database
    migrate    generate new migration and place in migrations folder
    diff       prints out the sql diff necessary to convert `source` to `target`
    clean      cleans the current instance of all temporary databases
    compact    ensure both database and migrations folder are current with schema and compact to
               only one migration
```

## What is `compact`?

Over time a migrations folder can get large and unwieldy, with possibly hundreds of migrations. This long log gets less and less useful over time, especially for small teams. The `compact` command replaces all migrations with a single migration that creates the entire schema at once.

Some teams will consider this dangerous and unnecessary, and they're free to not use it!

# Example

If your `schema` directory contains a sql file like this:

```sql
create table fruit (
  id serial primary key,
  name text not null unique,
  color text not null default ''
);
```

Then running `migrator migrate 'add fruit table'` will generate a migration called `$new_version.$previous_version.add_fruit_table.sql` in the `migrations` folder.

If you then change your schema sql to this:

```sql
create type flavor_type as enum('SWEET', 'SAVORY');

create table fruit (
  id serial primary key,
  name text not null unique,
  flavor flavor_type not null default 'SWEET'
);
```

Then running `migrator migrate 'remove color add flavor'` will generate `$new_version.$previous_version.remove_color_add_flavor.sql` that will go from the previous state to the new state.

# Credits

- [`migra`](https://github.com/djrobstep/migra) for making it possible to diff schemas.
- [`tusker`](https://github.com/bikeshedder/tusker) was the inspiration for using temporary databases as diff targets. `migrator` adds the ability to generate and run versioned migrations and to perform compaction.
- Thank you [Rust](https://www.rust-lang.org/) for being so awesome! [clap](https://github.com/clap-rs/clap) and [rust-postgres](https://github.com/sfackler/rust-postgres) in particular made this way easier.

# Contributing

Pull requests making the script more ergonomic or robust are welcome.
