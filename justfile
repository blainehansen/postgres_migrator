setup:
	cargo install cargo-bump just

dev:
	docker exec -it migrator-dev bash

test:
	docker exec -it migrator-dev cargo test

full_test:
	docker exec -it migrator-dev cargo test -- --ignored

build: test full_test
	docker build -f release.Dockerfile -t blainehansen/migrator .

integration_test: build
	#!/usr/bin/env bash
	set -euo pipefail
	PG_URL='postgres://experiment_user:asdf@localhost:5432/experiment_db?sslmode=disable'
	docker run --rm -it --network host -u $(id -u ${USER}):$(id -g ${USER}) -v $(pwd):/working -e PG_URL=$PG_URL blainehansen/migrator migrate

_status_clean:
	#!/usr/bin/env bash
	set -euo pipefail

	if [ -n "$(git status --porcelain)" ]; then
		echo "git status not clean"
		exit 1
	fi

release SEMVER_PORTION: _status_clean build integration_test
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
	docker tag blainehansen/migrator blainehansen/migrator:$VERSION
	docker push blainehansen/migrator:$VERSION
	docker push blainehansen/migrator:latest

	git push origin main
	git push origin main --tags
