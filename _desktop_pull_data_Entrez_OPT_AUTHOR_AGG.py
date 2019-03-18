import scrapy
from scrapy.crawler import CrawlerProcess
from scrapy.selector import Selector
import pandas as pd
import numpy as np
import datetime
import json
import sys

# Overall spider class that manages the spider # #  # # # # # DEPRICATED FOR NOW
class ChamberSpider(scrapy.Spider):
    DOWNLOAD_DELAY = 0.25
    name="chamber"
    start_urls = ["http://cm.pschamber.com/list/"]
    layer_to_stop = 1

    # First method called in scraping data
    def parse(self, response):

        artinfo = json.loads(response.text)
        artinfo = json.loads(response.text)
        result_set = artinfo["result"]
        found_pub_date = False
        global _update_new_id

        for uid in result_set:
            # UID must be an integer
            if uid == str(search_id):
                _update_new_id = search_id
                _pub_id = _update_new_id
                for above_element in result_set[str(uid)]:
                    if above_element == "authors":
                        for author_element in result_set[str(uid)]["authors"]:
                            df_author = author_element["name"]
                            # Add an entry to the dataframe for each author
                            add_data(_update_new_id, df_author, 0, 0, 0, 0)
                            _pub_authors.append(df_author)
                    elif above_element == "pubdate":
                        found_pub_date = True
                        _pub_year = result_set[str(uid)]["pubdate"].split()[0]
                    elif not found_pub_date and above_element == "epubdate":
                        _pub_year = result_set[str(uid)]["epubdate"].split()[0]
                    elif above_element == "title":
                        _pub_name = result_set[str(uid)]["title"]
                    elif above_element == "articleids":
                        for item in result_set[str(uid)]["articleids"]:
                            if item['idtype'] == "doi":
                                _pub_doi = item['value']
                                break
                    elif above_element == "sortfirstauthor":
                        _pub_fa = result_set[str(uid)]["sortfirstauthor"]
                    elif above_element == "lastauthor":
                        _pub_la = result_set[str(uid)]["lastauthor"]

            references_link = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/elink.fcgi?dbfrom=pubmed&id=" + str(_update_new_id) + "&retmode=json"
            print(references_link)
            yield scrapy.Request(references_link,
                                 callback=self.parse_references,
                                 meta={
                                     'id': _update_new_id,
                                     'layer': 1,
                                     'fb': 0,
                                 }
                                 )

    def parse_base_article_string(self, response):
        # Parse the JSON response
        pp_refs = json.loads(response.text)
        if "result" in pp_refs:
            result_set = pp_refs["result"]

            # Find all authors of each UID
            for uid in result_set:
                # UID must be an integer
                if uid != "uids":
                    df_uid = uid
                    df_date = "0"
                    print("uid",uid)

                    # Extract all authors
                    for above_element in result_set[str(uid)]:
                        if above_element == "authors":
                            for author_element in result_set[str(uid)]["authors"]:
                                df_author = author_element["name"]
                                # Add an entry to the dataframe for each author
                                if response.meta["layer"] == 0:
                                    add_data(df_uid, df_author, response.meta["layer"], df_date, df_uid, response.meta["fb"])
                                else:
                                    add_data(df_uid, df_author, response.meta["layer"], df_date, response.meta["parent"], response.meta["fb"])

    def parse_references(self, response):
        # Parse JSON
        pp_refs = json.loads(response.text)
        linkset = pp_refs["linksets"]

        link_dict = {}

        found_refs = False
        found_citedin = False

        # Extract publication IDs for the references (papers cited by this publication)
        for linksetdb in linkset:
            linksetdbs = linksetdb["linksetdbs"]
            for item in linksetdbs:
                #print(item)
                if item["linkname"] == "pubmed_pubmed_refs":  # Backwards
                    print(linksetdb)
                    pubmed_refs = item["links"]
                    found_refs = True
                    for link in pubmed_refs:
                        print(link)
                        link_dict[link] = -1
                    break

        # Extract publication IDs for the cited-in field (papers that cite this publication)
        for linksetdb in linkset:
            linksetdbs = linksetdb["linksetdbs"]
            for item in linksetdbs:
                print(item["linkname"])
                if item["linkname"] == "pubmed_pubmed_citedin":  # Forwards
                    found_citedin = True
                    pubmed_citedin = item["links"]
                    for link in pubmed_citedin:
                        link_dict[link] = 1
                    break

        keys = link_dict.keys()
        split = 20

        key_string_forwards = {0: ""}
        key_string_backwards = {0: ""}
        counter_forwards = 0
        counter_backwards = 0
        for key in keys:
            if link_dict[key] == -1:
                if len(key_string_backwards[counter_backwards] + str(key) + ",") >= 1024:
                    counter_backwards += 1
                key_string_backwards[counter_backwards] += str(key) + ","
            elif link_dict[key] == 1:
                if len(key_string_forwards[counter_forwards] + str(key) + ",") >= 1024:
                    counter_forwards += 1
                    key_string_forwards[counter_forwards] += str(key) + ","
        print("stringback", key_string_backwards)
        #exit(0)
        if len(key_string_backwards[0]) > 4:
            for keyb in key_string_backwards.keys():
                yield scrapy.Request(
                    "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esummary.fcgi?db=pubmed&id=" + key_string_backwards[
                        keyb] + "&retmode=json",
                    callback=self.parse_base_article_string,
                    meta={
                        'layer': response.meta["layer"] + 1,
                        'parent': 0,  # response.meta["id"],
                        'fb': -1,
                    }
                )
        if len(key_string_forwards[0]) > 4:
            for keyf in key_string_forwards.keys():
                yield scrapy.Request(
                    "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esummary.fcgi?db=pubmed&id=" + key_string_backwards[
                        keyf] + "&retmode=json",
                    callback=self.parse_base_article_string,
                    meta={
                        'layer': response.meta["layer"] + 1,
                        'parent': 0,  # response.meta["id"],
                        'fb': 1,
                    }
                )

        # for i in range(split):
        #     if i in key_string_backwards:
        #         search_string_b = key_string_backwards[i]
        #         yield scrapy.Request(
        #             "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esummary.fcgi?db=pubmed&id=" + search_string_b + "&retmode=json",
        #             callback=self.parse_base_article_string,
        #             meta={
        #                 'layer': response.meta["layer"] + 1,
        #                 'parent': 0,  # response.meta["id"],
        #                 'fb': -1
        #             }
        #             )
        #
        #     if i in key_string_forwards:
        #         search_string_f = key_string_forwards[i]
        #         yield scrapy.Request(
        #             "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esummary.fcgi?db=pubmed&id=" + search_string_f + "&retmode=json",
        #             callback=self.parse_base_article_string,
        #             meta={
        #                 'layer': response.meta["layer"] + 1,
        #                 'parent': 0,  # response.meta["id"],
        #                 'fb': 1
        #             }
        #             )

# -- -- -- -- -- -- -- -- -- -- Helper methods for data handling -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -

# Tracks the IDs that were dropped due to a lack of available citation data
dropped_rows = list()

# Adds data to the overall dataframe
def add_data(id, author, layer, date, parent, f_b):
    author = author.lower()

    # Create a temporary dataframe to hold the function's inputs
    temp_df = pd.DataFrame(data=[date, id, author, layer, parent, f_b]).transpose()
    temp_df.columns = ['date', 'id', 'name', 'layer', 'parent', 'fb']

    # Append the temporary dataframe to the global dataframe "gdf"
    global gdf
    gdf = gdf.append(temp_df)

    # Update the user on progress
    # print("Added new data point [" + str(id) + "; layer = " + str(layer) + "]")

# Drop a given ID's rows from a dataframe
def drop_rows(id):
    dropped_rows.append(id)
    global gdf
    gdf = gdf[gdf.id != id]

# Format a search term (replaces all spaces with "%20")
def format_search(word):
    return word.replace(" ", "%20")

def represents_int(s):
    for i in range(10):
        if s.startswith(str(i)):
            return True
    return False

# -- -- -- -- -- -- -- -- -- -- Script initiation code -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- --

first_arg = "Gary Benson"#sys.argv[1]
second_arg = "100"#sys.argv[2]
third_arg = "False"#sys.argv[3]
output_name = "ME"#sys.argv[4]

search_id = first_arg
to_id = second_arg
represents_string = "True" == third_arg

#search_id = 29656858

# Global dataframe which contains all data retrieved on parent and child publications
gdf = pd.DataFrame(columns=['date', 'id', 'name', 'layer', 'parent', 'fb'])

# Create a spider to perform the web crawling process
process = CrawlerProcess({
    'USER_AGENT': 'Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1)'
})

url = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esummary.fcgi?db=pubmed&id=" + str(search_id) + "&retmode=json"

ChamberSpider.start_urls = [ url ]
process.crawl(ChamberSpider)
x = process.start()

# Output the contents of the dataframe into a csv file
gdf.to_csv(path_or_buf="/Users/mkorovkin/Desktop/author_" + str(search_id) + ".csv", encoding='utf-8')

# Notify the user once data collection is finished
print("Finished data collection on the publication history of ID \"" + str(search_id) + "\"")
print("Dropped " + str(len(dropped_rows)) + " ids.")