setup:
	cargo install cargo-bump just

dev:
	docker exec -it migrator-dev bash

test:
	docker exec -it migrator-dev cargo test

full_test:
	docker exec -it migrator-dev cargo test -- --ignored

build:
	docker build -f release.Dockerfile -t blainehansen/postgres_migrator .

integration_test: test full_test build
	#!/usr/bin/env bash
	set -euo pipefail
	PG_URL='postgres://experiment_user:asdf@localhost:5432/experiment-db?sslmode=disable'
	docker run --rm -it --network host -u $(id -u ${USER}):$(id -g ${USER}) -v $(pwd):/working -e PG_URL=$PG_URL blainehansen/postgres_migrator migrate

compose_test:
	#!/usr/bin/env bash
	set -euo pipefail
	docker exec -it -u $(id -u ${USER}):$(id -g ${USER}) postgres_migrator postgres_migrator migrate

_status_clean:
	#!/usr/bin/env bash
	set -euo pipefail

	if [ -n "$(git status --porcelain)" ]; then
		echo "git status not clean"
		exit 1
	fi

release SEMVER_PORTION: _status_clean build integration_test
	#!/usr/bin/env bash
	set -euxo pipefail

	cargo bump {{SEMVER_PORTION}}

	VERSION=$(grep '^version = "' Cargo.toml)
	[[ $VERSION =~ ([0-9]+\.[0-9]+\.[0-9]+) ]]
	VERSION="${BASH_REMATCH[1]}"
	echo $VERSION
	GIT_VERSION="v$VERSION"
	echo $GIT_VERSION

	docker tag blainehansen/postgres_migrator blainehansen/postgres_migrator:$VERSION
	docker push blainehansen/postgres_migrator:$VERSION
	docker push blainehansen/postgres_migrator:latest
	git commit -am $GIT_VERSION
	git tag $GIT_VERSION
	cargo publish

	git push origin main
	git push origin main --tags
