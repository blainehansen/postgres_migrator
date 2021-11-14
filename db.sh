#!/bin/bash
set -euo pipefail
PROGRAM_NAME=$(basename $0)

show_help() {
	echo "Usage: $PROGRAM_NAME <subcommand>"
	echo "commands:"
	echo "    migrate <migration description>"
	echo "    up"
	echo "    compact"
	# echo "    update_this_script"
	echo ""
}

show_help_error() {
	show_help
	exit 1
}

db_compact() {
	PG_URL=$(grep 'url =' tusker.toml | sed "s/url = '\(.*\)'/\1/")

	db_migrate ensuring_current
	db_up

	rm -f ./migrations/*
	db_migrate compacted_initial
	MIGRATION_FILES=$(ls -1 ./migrations)
	VERSION_NUMBER="$(echo ${MIGRATION_FILES[0]} | cut -d'.' -f1)"
	echo "new version number is: $VERSION_NUMBER"

	psql $PG_URL -q -c "truncate table _schema_versions; insert into _schema_versions (version_number) values ($VERSION_NUMBER)"
}

db_migrate() {
	DESCRIPTION="$*"

	TIMESTAMP=$(date +"%Y%m%d%H%M%S")
	if [ -z "$DESCRIPTION" ]; then
		show_help_error
	fi
	DESCRIPTION=$(echo "$DESCRIPTION" | sed 's/\s\+/_/g' <<<"$*")
	echo "$DESCRIPTION"
	mkdir -p ./migrations
	MIGRATION_UP=$(tusker diff)
	echo "creating migration file:"
	echo "==="
	echo "$MIGRATION_UP"
	echo "==="

	echo "$MIGRATION_UP" > "./migrations/${TIMESTAMP}.${DESCRIPTION}.sql"
}

db_up() {
	PG_URL=$(grep 'url =' tusker.toml | sed "s/url = '\(.*\)'/\1/")

	PGOPTIONS="-c client_min_messages=error" psql $PG_URL -q -X -c "create table if not exists _schema_versions (version_number char(14) unique not null)"

	# grab the latest version
	# https://stackoverflow.com/questions/15242752/store-postgresql-result-in-bash-variable
	CURRENT_VERSION_NUMBER=$(psql $PG_URL -q -X -A -t -c "select max(version_number) from _schema_versions")
	echo "current version is: $CURRENT_VERSION_NUMBER"

	mkdir -p ./migrations
	MIGRATION_FILES=$(ls -1 ./migrations | sort)
	for MIGRATION_FILE in $MIGRATION_FILES
	do
		VERSION_NUMBER="$(echo $MIGRATION_FILE | cut -d'.' -f1)"
		if [ -z $CURRENT_VERSION_NUMBER ] || [[ $VERSION_NUMBER > $CURRENT_VERSION_NUMBER ]]; then
			echo "performing $MIGRATION_FILE"
			psql $PG_URL -q -f "./migrations/$MIGRATION_FILE"
			psql $PG_URL -q -c "insert into _schema_versions (version_number) values ($VERSION_NUMBER)"
		else
			echo "not performing $MIGRATION_FILE"
		fi
	done
}

# db_update_this_script() {
# 	# something with curl and the current location
# }

if [ "$#" -lt 1 ]; then
	show_help_error
fi

SUBCOMMAND="db_$1"
if declare -f "$SUBCOMMAND" >/dev/null 2>&1; then
	shift
	"$SUBCOMMAND" "$@"
else
	echo "unrecognized subcommand"
	echo ""
	show_help_error
fi
