import requests
import json


# Add a search method that traverses the APICrawler B-Tree

class APICrawler:
    def __init__(self, url, apiKey):
        self.url = url
        self.tree = {}
        self.crawl()
        self.apiKey = apiKey

    def crawl(self):
        r = requests.get(self.url)
        if r.status_code == 200:
            self.tree = json.loads(r.text)
            self.crawl_children(self.tree)
        else:
            print("Error: " + str(r.status_code))

    def crawl_children(self, node, dictKey):
        for child in node[dictKey]:
            r = requests.get(child['url'])
            if r.status_code == 200:
                child = json.loads(r.text)
                self.crawl_children(child)
            else:
                print("Error: " + str(r.status_code))

    def search(self, searchTerm):
        return self.search_children(self.tree, searchTerm)

    def search_children(self, node, searchTerm):
        for child in node:
            if child == searchTerm:
                return node[child]
            else:
                return self.search_children(node[child], searchTerm)
