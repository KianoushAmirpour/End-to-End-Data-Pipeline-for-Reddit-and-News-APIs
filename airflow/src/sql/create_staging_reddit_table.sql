create table if not exists STAGING.reddit
    (
    reddit_ids text,
    titles text,
    authors_name text,
    scores bigint,
    num_comments bigint,
    upvote_ratio float,
    created_utc TIMESTAMP,
    post_type text
    );