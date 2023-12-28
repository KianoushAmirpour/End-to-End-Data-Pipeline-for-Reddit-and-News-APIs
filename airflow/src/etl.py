import os
import json
import argparse
from datetime import date
from .news import NewsAPI
from .reddit import RedditAPI
from .s3_module import CloudStorage
from .utils import setup_logger
from .transform import TransformNewsData, TransformRedditData
import pandas as pd

loggr = setup_logger(__name__)

def main():
    
    landing_zone = os.environ.get("LANDING_ZONE_BUCKET_NAME")
    processed_zone = os.environ.get("PROCESSED_ZONE_BUCKET_NAME")

    s3storage = CloudStorage()
    
    news = NewsAPI().call_api()
    s3storage.upload_to_bucket(landing_zone, news, f"news-{'sports'}-{date.today()}", format='json')
    
    reddit_instance = RedditAPI()
    for post_type in ['new','hot','top']:
        posts = reddit_instance.get_posts_by_type(post_type)
        s3storage.upload_to_bucket(landing_zone, posts, f"reddit-{post_type}-{'sports'}-{date.today()}", format='json')
    
    files_in_landing_zone = s3storage.get_files(landing_zone)
    s3storage.clean_bucket(processed_zone)

    reddit_transform = TransformRedditData()
    for key in files_in_landing_zone:
        # data = s3storage.get_object(landing_zone, key)

        data = json.loads(s3storage.get_object(landing_zone, key))
        if key.startswith('news'):
            df_news = TransformNewsData().transform(data)
            s3storage.upload_to_bucket(processed_zone, df_news, "news", format="csv")
        else:
            post_type = key.split('-')[1]
            reddit_transform.transform(data, post_type)
    df_reddit = pd.concat(reddit_transform.dfs, ignore_index=True)
    s3storage.upload_to_bucket(processed_zone, df_reddit , "reddit", format="csv")

if __name__ == "__main__":
    main()



   
