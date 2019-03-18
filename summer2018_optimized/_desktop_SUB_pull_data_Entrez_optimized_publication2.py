import scrapy
from scrapy.crawler import CrawlerProcess
from scrapy.selector import Selector
import pandas as pd
import numpy as np
import datetime
import json
import sys

# Overall spider class that manages the spider
class ChamberSpider(scrapy.Spider):
    name="chamber"
    start_urls = ["http://cm.pschamber.com/list/"]
    layer_to_stop = 1

    # First method called in scraping data
    def parse(self, response):

        artinfo = json.loads(response.text)
        result_set = artinfo["esearchresult"]["idlist"]
        print(result_set)
        #f = open("/Users/mkorovkin/Desktop/" + new_id + ".txt", "w+")
        #f.write("https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esummary.fcgi?db=pubmed&id=" + new_id + "&retmode=json")
        #f.close()
        for new_id in result_set:
            link = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esummary.fcgi?db=pubmed&id=" + new_id + "&retmode=json"
            #print(link)
            yield scrapy.Request(link, callback=self.parse_base_article_string)

    def parse_base_article_string(self, response):
        global title_dict
        # Parse the JSON response
        pp_refs = json.loads(response.text)
        if "result" in pp_refs:
            result_set = pp_refs["result"]
            # Find all authors of each UID
            for uid in result_set:
                if uid != "uids":
                    if "title" in result_set[str(uid)]:
                        title_dict[uid] = result_set[str(uid)]["title"]


# -- -- -- -- -- -- -- -- -- -- Helper methods for data handling -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -

def represents_int(s):
    for i in range(10):
        if s.startswith(str(i)):
            return True
    return False

# -- -- -- -- -- -- -- -- -- -- Script initiation code -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- --

first_arg = sys.argv[1]
third_arg = sys.argv[2]

title_dict = {}

search_id = first_arg.replace("_", " ")
represents_string = "True" == third_arg

# Create a spider to perform the web crawling process
process = CrawlerProcess({
    'USER_AGENT': 'Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1)'
})

url = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esummary.fcgi?db=pubmed&id=" + str(search_id) + "&retmode=json"
if represents_string:
    url = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi?db=pubmed&term=" + search_id + "[title]&retmax=100&retmode=json"

ChamberSpider.start_urls = [ url ]
process.crawl(ChamberSpider)
x = process.start()

f = open("/Users/mkorovkin/Desktop/citations_" + str(first_arg) + "_info.txt","w+")
for key in title_dict.keys():
    f.write(key + "|||{split}|||" + title_dict[key] + "\n")
f.close()