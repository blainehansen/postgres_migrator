set -euo pipefail

# PGPASSWORD="asdf" psql -U experiment_user -h localhost experiment_db -c "select 1 as one"

assert () {
	E_PARAM_ERR=98
	E_ASSERT_FAILED=99

	if [ -z "$2" ]
	then
		return $E_PARAM_ERR
	fi

	lineno=$2

	if [ ! $1 ]
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

PG_URL='postgres://experiment_user:asdf@localhost:5432/experiment_db?sslmode=disable'

rm -f ./migrations/*
psql $PG_URL -c "drop schema public cascade; create schema public; grant all on schema public to public; comment on schema public is 'standard public schema'"

# target schema.1
sed -i '2s/.*/filename = "schemas\/schema.1.sql"/' tusker.toml

./db.sh migrate 'one'
assert_migration_count 1 $LINENO

./db.sh up
psql $PG_URL -c "select id, name, color from fruit"

# target schema.2
sed -i '2s/.*/filename = "schemas\/schema.2.sql"/' tusker.toml
./db.sh migrate 'two'
assert_migration_count 2 $LINENO
./db.sh up
psql $PG_URL -c "select id, name, flavor from fruit"

# target schema.3
sed -i '2s/.*/filename = "schemas\/schema.3.sql"/' tusker.toml
./db.sh compact
assert_migration_count 1 $LINENO
psql $PG_URL -c "select person.name, fruit.name, flavor from person join fruit on person.favorite_fruit = fruit.id where flavor = 'SALTY'"

# target schema.1
sed -i '2s/.*/filename = "schemas\/schema.1.sql"/' tusker.toml
./db.sh migrate 'back to one'
assert_migration_count 2 $LINENO
./db.sh up
psql $PG_URL -c "select id, name, color from fruit"
