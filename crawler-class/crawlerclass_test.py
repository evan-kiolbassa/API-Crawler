import unittest

class TestAPICrawler(unittest.TestCase):
    def setUp(self):
        self.crawler = APICrawler("https://swapi.co/api/", "")

    def test_crawl(self):
        self.assertEqual(self.crawler.tree['name'], "Luke Skywalker")

    def test_search(self):
        self.assertEqual(self.crawler.search("name"), "Luke Skywalker")

if __name__ == '__main__':
    unittest.main()
