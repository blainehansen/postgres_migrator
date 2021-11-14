create type flavor_type as enum('SWEET', 'SAVORY');

create table fruit (
	id serial primary key,
	name text not null unique,
	flavor flavor_type not null default 'SWEET'
);
