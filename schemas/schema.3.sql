create type flavor_type as enum('SWEET', 'SAVORY', 'SALTY');

create table fruit (
	id serial primary key,
	name text not null unique,
	flavor flavor_type not null default 'SWEET'
);

create table person (
	name text primary key,
	favorite_fruit int references fruit(id)
);
