import unittest

class TestCrawler(unittest.TestCase):

    def setUp(self):
        self.crawler = Crawler('your_api_key_here')

    def test_empty_search(self):
        # Test that search with empty string returns an empty list
        self.assertEqual(self.crawler.search(''), [])

    def test_invalid_search(self):
        # Test that search with invalid keyword returns an empty list
        self.assertEqual(self.crawler.search('invalid keyword'), [])

    def test_invalid_series_id(self):
        # Test that get_series with invalid series ID raises an exception
        with self.assertRaises(Exception):
            self.crawler.get_series('INVALID_SERIES_ID')

    def test_crawl_top_level_category(self):
        # Test that crawl with top level category ID returns non-empty B-Tree
        self.crawler.crawl(371)
        self.assertGreater(len(self.crawler.series_ids), 0)

    def test_crawl_invalid_category(self):
        # Test that crawl with invalid category ID raises an exception
        with self.assertRaises(Exception):
            self.crawler.crawl(-1)

    def test_fetch_all_series(self):
        # Test that fetch_all_series returns a dictionary with at least one key-value pair
        self.crawler.crawl(371)
        responses = self.crawler.fetch_all_series()
        self.assertIsInstance(responses, dict)
        self.assertGreater(len(responses), 0)

    def test_crawl(self):
        self.crawler.crawl(self.category_id)
        self.assertIsInstance(self.crawler.series_ids, AVLTree)
        self.assertGreater(len(self.crawler.series_ids), 0)
        self.assertIn(self.category_id, self.crawler.series_ids)

    def test_search(self):
        self.crawler.crawl(self.category_id)
        results = self.crawler.search('brent')
        self.assertIsInstance(results, AVLTree)
        self.assertGreater(len(results), 0)
        for key in results:
            self.assertIsInstance(key, str)
            self.assertTrue('BRENT' in key.upper())

    def test_fetch_all_series(self):
        self.crawler.crawl(self.category_id)
        responses = self.crawler.fetch_all_series()
        self.assertIsInstance(responses, dict)
        for key in responses:
            self.assertIsInstance(key, tuple)
            self.assertEqual(len(key), 2)
            self.assertIsInstance(key[0], str)
            self.assertIsInstance(key[1], int)
            self.assertIsInstance(responses[key], dict)
            self.assertIn('series', responses[key])
            self.assertIn('data', responses[key]['series'])
            self.assertIsInstance(responses[key]['series']['data'], list)

    def test_get_brent_market_kpis(self):
        self.crawler.crawl(self.category_id)
        results = self.crawler.search('brent')
        responses = self.crawler.fetch_selected_series(results)
        for key, response in responses.items():
            series_id = key[0]
            series_name = response['series'][0]['name']
            series_data = response['series'][0]['data']
            print(f"{series_name} ({series_id})")
            print(f"Last Value: {series_data[-1][1]}")
            print("")

if __name__ == '__main__':
    unittest.main()
