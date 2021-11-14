create table fruit (
	id serial primary key,
	name text not null unique,
	color text not null default ''
);
