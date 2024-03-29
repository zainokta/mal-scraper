create table if not exists animes(
	uuid uuid primary key default gen_random_uuid(),
	title varchar(255) unique not null,
	link varchar(255) not null,
	image varchar(255) not null,
	genres varchar[],
	episodes varchar(10) not null,
	studios varchar[] not null,
	status varchar(50) not null
);
