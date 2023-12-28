create table if not exists WAREHOUSE.news
    (
    id serial primary key,
    sources varchar(255) not null,
    titles text,
    urls text
    );