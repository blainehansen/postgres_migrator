setup:
	cargo install cargo-bump just

dev:
	docker exec -it migrator bash

build:
	docker build -t blainehansen/migrator .

test:
	cargo test

_status_clean:
	#!/usr/bin/env bash
	set -euo pipefail

	if [ -n "$(git status --porcelain)" ]; then
		echo "git status not clean"
		exit 1
	fi

release SEMVER_PORTION: _status_clean test build integration_test
	#!/usr/bin/env bash
	set -euo pipefail

	cargo bump {{SEMVER_PORTION}}

	VERSION=$(grep '^version = "' Cargo.toml)
	[[ $VERSION =~ ([0-9]+\.[0-9]+\.[0-9]+) ]]
	VERSION="${BASH_REMATCH[1]}"
	echo $VERSION
	GIT_VERSION="v$VERSION"
	echo $GIT_VERSION

	git commit -am $GIT_VERSION
	git tag $GIT_VERSION
	docker push blainehansen/migrator:$(VERSION)
	docker push blainehansen/migrator:latest

	git push origin main
	git push origin main --tags


integration_test: build
	#!/usr/bin/env bash
	set -euo pipefail

	function migrator {
		local result=$(docker run --rm -it --network host -u $(id -u ${USER}):$(id -g ${USER}) -v $(pwd):/working blainehansen/migrator "$@")
		echo $result
	}

	assert () {
		E_PARAM_ERR=98
		E_ASSERT_FAILED=99

		if [ -z "$2" ]
		then
			return $E_PARAM_ERR
		fi

		lineno=$2

		if [ ! "$1" ]
		then
			echo "Assertion failed:  \"$1\""
			echo "File \"$0\", line $lineno"
			exit $E_ASSERT_FAILED
		fi
	}

	assert_migration_count () {
		MIGRATION_COUNT=$(ls ./migrations -al | wc -l)
		MIGRATION_COUNT="$((MIGRATION_COUNT - 3))"
		assert "$MIGRATION_COUNT -eq $1" $2
	}

	# PGPASSWORD="asdf" psql -U experiment_user -h localhost experiment_db -c "select 1 as one"
	PG_URL='postgres://experiment_user:asdf@localhost:5432/experiment_db?sslmode=disable'

	rm -f ./migrations/*
	psql $PG_URL -c "drop schema public cascade; create schema public; grant all on schema public to public; comment on schema public is 'standard public schema'"

	DIFF=$(migrator --dbname "experiment_db" --user "experiment_user" --password "asdf" --schema-directory schemas/schema.1 diff database schema)
	assert "$DIFF" $LINENO

	# schema.1
	migrator --dbname "experiment_db" --user "experiment_user" --password "asdf" --schema-directory schemas/schema.1 migrate 'one'
	assert_migration_count 1 $LINENO
	migrator --dbname "experiment_db" --user "experiment_user" --password "asdf" up
	psql $PG_URL -c "select id, name, color from fruit"

	# schema.2
	migrator --dbname "experiment_db" --user "experiment_user" --password "asdf" --schema-directory schemas/schema.2 migrate 'two'
	assert_migration_count 2 $LINENO
	migrator --dbname "experiment_db" --user "experiment_user" --password "asdf" up
	psql $PG_URL -c "select id, name, flavor from fruit"

	# schema.3
	migrator --dbname "experiment_db" --user "experiment_user" --password "asdf" --schema-directory schemas/schema.3 compact
	assert_migration_count 1 $LINENO
	psql $PG_URL -c "select person.name, fruit.name, flavor from person join fruit on person.favorite_fruit = fruit.id where flavor = 'SALTY'"

	# schema.1
	migrator --dbname "experiment_db" --user "experiment_user" --password "asdf" --schema-directory schemas/schema.1 migrate 'back to one'
	assert_migration_count 2 $LINENO
	migrator --dbname "experiment_db" --user "experiment_user" --password "asdf" up
	psql $PG_URL -c "select id, name, color from fruit"

	migrator --dbname "experiment_db" --user "experiment_user" --password "asdf" clean
	psql $PG_URL -c "create database garbage_tmp"
	psql $PG_URL -c "comment on database garbage_tmp is 'TEMP DB CREATED BY migrator'"
	migrator --dbname "experiment_db" --user "experiment_user" --password "asdf" clean
	# this is just a ghetto way to make sure `clean` actually removes garbage_tmp, since this command will fail otherwise
	psql $PG_URL -c "create database garbage_tmp"
	psql $PG_URL -c "drop database garbage_tmp"
