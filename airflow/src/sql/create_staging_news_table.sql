create table if not exists STAGING.news
    (
    sources varchar(255),
    titles text,
    urls text
    );