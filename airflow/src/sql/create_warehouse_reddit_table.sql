create table if not exists WAREHOUSE.reddit
   (
   post_id serial primary key,
   reddit_ids text not null,
   authors_name text not null,
   titles text,
   scores int,
   num_comments int,
   upvote_ratio float,
   created_utc timestamp,
   post_type text
   );

