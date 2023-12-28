BEGIN TRANSACTION;

DELETE FROM WAREHOUSE.reddit
USING STAGING.reddit
WHERE WAREHOUSE.reddit.reddit_ids = STAGING.reddit.reddit_ids AND
      WAREHOUSE.reddit.created_utc = STAGING.reddit.created_utc AND
      WAREHOUSE.reddit.post_type = STAGING.reddit.post_type;

INSERT INTO WAREHOUSE.reddit (reddit_ids, authors_name, titles, scores, num_comments, upvote_ratio, created_utc, post_type)
SELECT reddit_ids, authors_name, titles, scores, num_comments, upvote_ratio, created_utc, post_type
FROM STAGING.reddit;

END TRANSACTION;
COMMIT;