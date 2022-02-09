build:
	docker build -t blainehansen/migrator .

run:
	docker run --rm -v ${PWD}:/working blainehansen/migrator
