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
        # Extract the PubMed IDs of a given author's work (see script initiation for clarity)
        link_refs = json.loads(response.text)
        e_search_result = link_refs["esearchresult"]
        link_set = e_search_result["idlist"]

        # Assemble PubMed IDs into a list of publications to process through future spider calls
        id_string = ""
        id_list_int = list()
        id_list_strings = list()
        for id in link_set:
            indiv_id = np.asscalar(np.float32(id))
            id_list_int.append(indiv_id)
            string_to_append = (u'%d' % indiv_id)
            id_list_strings.append(string_to_append)

        for ix in range(len(id_list_strings)):
            if ix != 0 and ix % 15 == 0:
                article_information_link = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esummary.fcgi?db=pubmed&id=" + id_string + "&retmode=json"
                references_link = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/elink.fcgi?dbfrom=pubmed&id=" + id_string + "&retmode=json"
                yield scrapy.Request(article_information_link,
                                     callback=self.parse_base_article_string,
                                     meta={
                                         'layer': 0,
                                         'fb': 0,
                                     }
                                     )
                yield scrapy.Request(references_link,
                                     callback=self.parse_parent_id_string,
                                     meta={
                                         'link': article_information_link,
                                         'layer': 0,
                                         'fb': 0,
                                     }
                                     )
                print(id_string)
                id_string = ""
            else:
                id_string += id_list_strings[ix] + ","

    def parse_base_article_string(self, response):
        # Parse the JSON response
        pp_refs = json.loads(response.text)
        result_set = pp_refs["result"]

        # Find all authors of each UID
        for uid in result_set:
            # UID must be an integer
            if uid != "uids":
                df_uid = uid
                df_date = "0"
                if False:
                    df_date = result_set[str(uid)]["pubdate"]

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

    def parse_parent_id_string(self, response):
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
                if item["linkname"] == "pubmed_pubmed_refs":  # Backwards
                    pubmed_refs = item["links"]
                    print(pubmed_refs)
                    found_refs = True
                    for link in pubmed_refs:
                        link_dict[link] = -1
                    break

        # Extract publication IDs for the cited-in field (papers that cite this publication)
        for linksetdb in linkset:
            linksetdbs = linksetdb["linksetdbs"]
            for item in linksetdbs:
                if item["linkname"] == "pubmed_pubmed_citedin":  # Forwards
                    found_citedin = True
                    pubmed_citedin = item["links"]
                    for link in pubmed_citedin:
                        link_dict[link] = 1
                    break

        # All following opporations are performed using dictionary "link_dict"
        keys = link_dict.keys()

        # Drop the publication from the dataframe if it has no listed references or cited-in
        if not found_refs and not found_citedin:
            drop_rows(response.meta["id"])
            return

        # Perform a recursive function call of the same nature as found in "parse"
        key_string_forwards = {}
        key_string_backwards = {}
        counter_forwards = 0
        counter_backwards = 0
        for key in keys:
            if link_dict[key] == -1:
                if not counter_backwards % 10 in key_string_backwards:
                    key_string_backwards[counter_backwards % 10] = str(key) + ","
                else:
                    key_string_backwards[counter_backwards % 10] += str(key) + ","
                counter_backwards += 1
            elif link_dict[key] == 1:
                if not counter_forwards % 10 in key_string_forwards:
                    key_string_forwards[counter_forwards % 10] = str(key) + ","
                else:
                    key_string_forwards[counter_forwards % 10] += str(key) + ","
                counter_forwards += 1

        for i in range(10):
            if i in key_string_backwards:
                search_string_b = key_string_backwards[i]
                yield scrapy.Request("https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esummary.fcgi?db=pubmed&id=" + search_string_b + "&retmode=json",
                                     callback=self.parse_base_article_string,
                                     meta={
                                         'layer': response.meta["layer"] + 1,
                                         'parent': 0,  # response.meta["id"],
                                         'fb': -1
                                     }
                                     )

            if i in key_string_forwards:
                search_string_f = key_string_forwards[i]
                yield scrapy.Request("https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esummary.fcgi?db=pubmed&id=" + search_string_f + "&retmode=json",
                                     callback=self.parse_base_article_string,
                                     meta={
                                         'layer': response.meta["layer"] + 1,
                                         'parent': 0, # response.meta["id"],
                                         'fb': 1
                                     }
                                     )

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

def format_author(word):
    to_return = word[(word.index(" ") + 1):] + " " + word[0]
    return to_return.lower()

# -- -- -- -- -- -- -- -- -- -- Script initiation code -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- --

# Maximum number of parent publications to investigate returned by the Entrez API
retmax = 100
year = None

first_arg = sys.argv[1]
second_arg = sys.argv[2]

search_name = first_arg
to_id = second_arg
# Name of the author to investigate

# Global dataframe which contains all data retrieved on parent and child publications
gdf = pd.DataFrame(columns=['date', 'id', 'name', 'layer', 'parent', 'fb'])

# Create a spider to perform the web crawling process
process = CrawlerProcess({
    'USER_AGENT': 'Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1)'
})
url = ""
if year is None:
    url = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi?db=pubmed&term=" + format_search(search_name) + "[author]&retmax=" + str(retmax) + "&retmode=json"
else:
    url = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi?db=pubmed&term=" + format_search(search_name) + "[author]" + year + "[pubdate]&retmax=" + str(retmax) + "&retmode=json"

print(format_search(search_name))

ChamberSpider.start_urls = [ url ]
process.crawl(ChamberSpider)
x = process.start()

# Output the contents of the dataframe into a csv file
file_name = to_id#str(search_name) + "_data_ms" + str(datetime.datetime.now().microsecond)
gdf.to_csv(path_or_buf="/Users/mkorovkin/Desktop/citations_" + file_name + ".csv", encoding='utf-8')
print(file_name + ".csv")

# Notify the user once data collection is finished
print("Finished data collection on the publication history of author \"" + search_name + "\"")
print("Dropped " + str(len(dropped_rows)) + " ids.")