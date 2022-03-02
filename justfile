build:
	docker build -t blainehansen/migrator .

run:
	docker exec -it migrator bash
