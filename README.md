```bash
alias migrator="docker exec -it --rm blainehansen/migrator"

migrator
```






This repo is a postgres migration runner that uses migra under to generate raw sql migrations from a single source of "declarative" sql file(s).

The script:

- Generates migrations in the format `$date_timestamp.$description.sql` into a `migrations` directory, using the `migrate <description>` subcommand.
- Runs all unperformed migrations and inserts the version number of each into a `_schema_versions` table, using the `up` subcommand.
- Compacts all migrations into a single migration, using the `compact` subcommand.

Turns out generating and running migrations isn't that hard! Especially if:

- You don't bother making migrations reversable, and instead just make a new migration if you want to undo something (that's best practice anyway).
- You don't worry about only running migrations up to a certain version.
- Schema diffing is taken care of by someone else! Thank you [`migra`](https://github.com/djrobstep/migra)!

Pull requests making the script more ergonomic or robust are welcome.
