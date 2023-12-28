BEGIN TRANSACTION;

DELETE FROM WAREHOUSE.news
USING STAGING.news
WHERE WAREHOUSE.news.sources = STAGING.news.sources; 

INSERT INTO WAREHOUSE.news (sources, titles, urls)
select sources, titles, urls from STAGING.news;

END TRANSACTION;
COMMIT;