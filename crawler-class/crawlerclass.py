import requests
from sortedcontainers import SortedDict
import multiprocessing as mp
from requests.exceptions import HTTPError, RequestException
import json
from time import time

class API_Crawler:
    def __init__(self, api_key):
        self.api_key = api_key
        self.base_url = 'http://api.eia.gov/'
        self.session = requests.Session()
        self.headers = {'Content-Type': 'application/json'}
        self.category_ids = SortedDict()

    def crawl(self, category_id, parent_id=None, retries=3, retry_delay=1):
        '''
        Recursively crawl the EIA API and store the resulting series IDs in a B-tree.
        Parameters:
        category_id (int): ID of the category to crawl.
        parent_id (int): ID of the parent category. Used for recursive crawling.
        retries (int): Number of times to retry the request if an error occurs.
        retry_delay (int): Delay in seconds between retries.
        Raises:
        requests.exceptions.HTTPError: If the API returns an error response or a response in an unexpected format.
        requests.exceptions.RequestException: If a network error occurs.
        '''
        if parent_id is None:
            self.category_ids.clear()
        else:
            self.category_ids[parent_id] = SortedDict()

        payload = {
            'api_key': self.api_key,
            'category_id': category_id
        }

        for i in range(retries):
            try:
                response = self.session.get(self.base_url + 'category/', headers=self.headers, params=payload)
                response.raise_for_status()
                category = response.json()['category']
            except requests.exceptions.HTTPError as e:
                print(f"Error while fetching category {category_id}: {e}")
                if response.status_code == 429:
                    print(f"Rate limit reached. Retrying in {retry_delay} seconds.")
                    time.sleep(retry_delay)
                    continue
                else:
                    raise
            except (json.JSONDecodeError, KeyError) as e:
                print(f"Error while parsing response for category {category_id}: {e}")
                raise
            except requests.exceptions.RequestException as e:
                print(f"Error while fetching category {category_id}: {e}")
                if i < retries - 1:
                    print(f"Retrying in {retry_delay} seconds.")
                    time.sleep(retry_delay)
                    continue
                else:
                    raise

            if 'childcategories' in category:
                for child in category['childcategories']:
                    self.crawl(child['category_id'], category_id, retries=retries, retry_delay=retry_delay)

            if 'childseries' in category:
                for series in category['childseries']:
                    self.category_ids[parent_id][series['series_id']] = None

    def search(self, keyword):
        """
        Search for a keyword in the stored series IDs.

        Parameters:
        keyword (str): The keyword to search for.

        Returns:
        A list of matching series IDs.
        """
        matches = []

        for parent_id, tree in self.category_ids.items():
            for series_id in tree.keys():
                if keyword in series_id:
                    matches.append((series_id, parent_id))

        return matches

    def _fetch_series(self, series_id, parent_id):
        """
        Fetch data for a single series ID and store the resulting JSON response in a dictionary.

        Parameters:
        series_id (str): ID of the series to fetch.
        parent_id (int): ID of the parent category.

        Returns:
        dict: A dictionary containing the JSON response from the API.
        """
        try:
            payload = {
                'api_key': self.api_key,
                'series_id': series_id
            }
            response = self.session.get(self.base_url + 'series/', headers=self.headers, params=payload)
            response.raise_for_status()
            return {(series_id, parent_id): response.json()}
        except requests.exceptions.HTTPError as e:
            print(f"Error while fetching series {series_id}: {e}")
            return {}
        except requests.exceptions.RequestException as e:
            print(f"Error while fetching series {series_id}: {e}")
            return {}

    def fetch_all_series(self, matches):
        """
        Fetch data for all series IDs in the search matches and store the resulting JSON responses in a dictionary.

        Parameters:
        matches (list): A list of tuples containing the series IDs and their parent category IDs.

        Returns:
        dict: A dictionary containing the JSON responses from the API.
        """
        responses = {}
        pool = mp.Pool(processes=mp.cpu_count())
        for series_id, parent_id in matches:
            pool.apply_async(self._fetch_series, args=(series_id, parent_id), callback=responses.update)
        pool.close()
        pool.join()
        return responses
