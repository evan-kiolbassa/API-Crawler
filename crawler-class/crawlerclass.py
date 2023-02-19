import requests
from queue import Queue
import multiprocessing as mp
from requests.exceptions import HTTPError, RequestException
import json
from bintrees import AVLTree
from time import time
from collections import defaultdict


class TrieNode:
    def __init__(self):
        self.children = defaultdict(TrieNode)
        self.is_word = False

class EIA_Crawler:
    def __init__(self, api_key):
        self.api_key = api_key
        self.base_url = 'http://api.eia.gov/'
        self.session = requests.Session()
        self.headers = {'Content-Type': 'application/json'}
        self.category_ids = AVLTree()

    def crawl(self, category_id, parent_id=None, retries=3, retry_delay=4):
            '''
            crawl the EIA API and store the resulting series IDs in an AVL tree.
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
                self.category_ids = TrieNode()
            else:
                self.category_ids.children[parent_id] = TrieNode()

            stack = [(category_id, parent_id)]

            while stack:
                category_id, parent_id = stack.pop()

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
                            return None
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
                            self.category_ids.children[parent_id].children[child['category_id']].is_word = False
                            stack.append((child['category_id'], parent_id))

                    if 'childseries' in category:
                        for series in category['childseries']:
                            self.category_ids.children[parent_id].children[series['series_id']].is_word = True

    def get_category_ids(self):
        """
        Returns the category IDs collected by the Crawler.
        """
        return self.category_ids

    def reset_category_ids(self):
        """
        Clears the category IDs collected by the Crawler.
        """
        self.category_ids.clear()

    def search(self, keywords):
        """
        Search for a keyword in the stored series IDs.

        Parameters:
        keyword (str): The keyword to search for.

        Returns:
        A list of matching series IDs.

        Start by adding the root node of the trie to the queue,
        and then loop until the queue is empty. For each node in the queue,
        check if its value (the series ID) contains the keyword.
        If it does, add the series ID and its parent ID to the matches list.

        Then, add all the children of the current node to the queue,
        so that it can be visited in the next iteration of the loop. This way,
        we traverse the entire trie and find all the series IDs that contain the keyword.
        """
        matches = []
        queue = Queue()
        queue.put(self.category_ids.root)

        while not queue.empty():
            node = queue.get()
            for child in node.children.values():
                for keyword in keywords:
                    if keyword in child.value:
                        matches.append((child.value, node.key))
                        break
                queue.put(child)


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
        for series_id, parent_id in matches:
            responses.update(self._fetch_series(series_id, parent_id))
        return responses

    def concatenate_series(self, series_data):
        """
        Concatenate the data for multiple series into a JSON format where each series ID is a column.
        Parameters:
        series_data (dict): A dictionary containing the JSON responses from the API for multiple series.
        Returns:
        dict: A dictionary containing the concatenated data.
        """
        concatenated_data = {}
        for series_id, data in series_data.items():
            if not data or 'series' not in data or not data['series']:
                continue
            series = data['series'][0]
            col_name = series['name']
            for i, val in enumerate(series['data']):
                if i not in concatenated_data:
                    concatenated_data[i] = {'time': val[0]}
                concatenated_data[i][col_name] = val[1]
        return list(concatenated_data.values())

