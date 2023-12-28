import os
import json
import requests
from typing import Dict, Union, List
from . import utils


class NewsAPI:

    URL = "https://newsapi.org/v2/top-headlines/"

    def __init__(self, category: str = 'sports') -> None:

        self.logger = utils.setup_logger(__name__)
        self.category = category
        self.api_key = os.environ.get("NEWS_API")
        self.params = {
            "language": "en",
            "pageSize": 100,
            "category": self.category,
            "apiKey": self.api_key
        }

    def call_api(self) -> Dict[str, Union[str, int, List]]:

        try:
            response = requests.get(self.URL, params=self.params)
            return json.dumps(response.json())
        except requests.exceptions.RequestException as e:
            self.logger.exception(
                f"Connection to NewsAPI was unsuccessful due to: {str(e)}.")


if __name__ == "__main__":
    s = NewsAPI().call_api()
