import os
import praw
import json
from . import utils
from typing import Dict, Any


class RedditAPI:
    def __init__(self, subreddit: str = 'sports', limit: int = None):

        self.client_id = os.environ.get("REDDIT_CLIENT_ID")
        self.secret = os.environ.get("REDDIT_SECRET")
        self.subreddit = subreddit
        self.limit = limit
        self.logger = utils.setup_logger(__name__)
        self.reddit_instance = self._connect()
        self.sub_reddit_instance = self.reddit_instance.subreddit(
            self.subreddit)

    def _connect(self):
        
        try:
            reddit = praw.Reddit(
                client_id=self.client_id,
                client_secret=self.secret,
                user_agent="my_app by u/levititan2023"
            )
            return reddit
        except praw.exceptions.PRAWException as e:
            self.logger.exception(
                f"Connection to RedditAPI was unsuccessful due to: {str(e)}.")
        except Exception as e:
            self.logger.exception(
                f"Connection to RedditAPI was unsuccessful due to: {str(e)}.")

    def _extract_features(self, posts) -> Dict[str, Any]:
        
        features = {}
        for post in posts:
            post_dict = vars(post)
            features[post_dict['id']] = {
                "title": str(post_dict["title"]),
                "author": str(post_dict["author"]),
                "score": post_dict["score"],
                "num_comments": post_dict["num_comments"],
                "selftext": str(post_dict["selftext"]),
                "created_utc": post_dict["created_utc"],
                "upvote_ratio": post_dict["upvote_ratio"],
                "over_18": post_dict["over_18"],
                "edited": post_dict["edited"],
                "spoiler": post_dict["spoiler"],
                "stickied": post_dict["stickied"]
            }
        return json.dumps(features, indent=4)

    def _get_top_posts(self) -> Dict[str, Any]:
        
        try:
            top_posts = self.sub_reddit_instance.top(limit=self.limit)
            return top_posts
        except Exception as e:
            self.logger.exception(
                f"Fetching the 'top posts' from RedditAPI was unsuccessful due to: {str(e)}.")

    def _get_hot_posts(self) -> Dict[str, Any]:
        
        try:
            hot_posts = self.sub_reddit_instance.hot(limit=self.limit)
            return hot_posts
        except Exception as e:
            self.logger.exception(
                f"Fetching the 'hot posts' from RedditAPI was unsuccessful due to: {str(e)}.")

    def _get_new_posts(self) -> Dict[str, Any]:
        
        try:
            new_posts = self.sub_reddit_instance.new(limit=self.limit)
            return new_posts
        except Exception as e:
            self.logger.exception(
                f"Fetching the 'new posts' from RedditAPI was unsuccessful due to: {str(e)}.")

    def get_posts_by_type(self, post_type: str) -> Dict[str, Any]:
        
        post_type_mappings = {
            'top': self._get_top_posts,
            'hot': self._get_hot_posts,
            'new': self._get_new_posts
        }
        if post_type in post_type_mappings:
            posts = post_type_mappings[post_type]()
            return self._extract_features(posts)
        else:
            raise ValueError(f"Invalid post type: {post_type}.")


if __name__ == "__main__":
    s = RedditAPI('sports').get_posts_by_type('new')
    print(s)
