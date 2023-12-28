from . import utils
import pandas as pd
from typing import Dict, Any
from datetime import datetime


class CommonCleaning:

    @staticmethod
    def standardize_quotes(text: str) -> str:

        quotes = ['“', '”', '‘', '’', '\"']
        for quote in quotes:
            if quote in text:
                text = text.replace(quote, "'")
        return text

    @staticmethod
    def remove_new_lines(text: str) -> str:

        return text.replace('\n', "")

    @staticmethod
    def remove_special_chars(text: str) -> str:

        return text.replace('=', "")


class TransformNewsData(CommonCleaning):

    def __init__(self):

        self.data = {'sources': [], 'titles': [],
                     'urls': []}
        self.logger = utils.setup_logger(__name__)

    def transform(self, data: Dict[str, Any]) -> pd.DataFrame:

        try:
            for news in data['articles']:
                self.data['sources'].append(news['source']['name'])
                self.data['titles'].append(news['title'])
                self.data['urls'].append(news['url'])
            df = pd.DataFrame.from_dict(self.data, orient='index')
            df = df.transpose()
            for col in df.columns:
                if col not in ["published_at"]:
                    df[col] = df[col].fillna("N/A")
                    df[col] = df[col].replace("", "N/A")
                    df[col] = df.apply(
                        lambda row: self.remove_new_lines(row[col]), axis=1)
                    df[col] = df.apply(
                        lambda row: self.standardize_quotes(row[col]), axis=1)
                    df[col] = df.apply(
                        lambda row: self.remove_special_chars(row[col]), axis=1)
            return df
        except Exception as e:
            self.logger.exception(
                f"Sth Unexpected happened while transformig 'news' due to: {str(e)}.")


class TransformRedditData(CommonCleaning):

    def __init__(self) -> None:

        self.data = {"ids": [],
                     "titles": [],
                     "authors": [],
                     "scores": [],
                     "num_comments": [],
                     "upvote_ratio": [],
                     "created_utc": [],
                     }
        self.dfs = []
        self.logger = utils.setup_logger(__name__)

    def transform(self, data: Dict[str, Dict], post_type: str) -> pd.DataFrame:

        try:
            for key in data.keys():
                self.data['ids'].append(key)
                self.data['authors'].append(data[key]['author'])
                self.data['titles'].append(data[key]['title'])
                self.data['scores'].append(data[key]['score'])
                self.data['num_comments'].append(data[key]['num_comments'])
                self.data['upvote_ratio'].append(data[key]['upvote_ratio'])
                self.data['created_utc'].append(
                    datetime.fromtimestamp((data[key]['created_utc'])).date())
            df = pd.DataFrame.from_dict(self.data, orient='index')
            df = df.transpose()
            df['post_type'] = post_type
            for col in df.columns:
                if col in ["titles", "authors"]:
                    df[col] = df[col].fillna("N/A")
                    df[col] = df[col].replace("", "N/A")
                    df[col] = df.apply(
                        lambda row: self.remove_new_lines(row[col]), axis=1)
                    df[col] = df.apply(
                        lambda row: self.standardize_quotes(row[col]), axis=1)
                    df[col] = df.apply(
                        lambda row: self.remove_special_chars(row[col]), axis=1)
            self.dfs.append(df)
        except Exception as e:
            self.logger.info(
                f"Sth Unexpected happened while transformig 'reddit' due to: {str(e)}.")
