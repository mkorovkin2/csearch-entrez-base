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
    DOWNLOAD_DELAY = 0.25
    start_urls = ["http://cm.pschamber.com/list/"]
    layer_to_stop = 1

    # First method called in scraping data
    def parse(self, response):
        global search_id
        global _pub_year
        global _pub_id
        global _pub_name
        global _pub_doi
        global _pub_la
        global _pub_fa
        global _pub_authors

        global alert_name

        artinfo = json.loads(response.text)
        if alert_name:
            global _update_new_id

            result_set = artinfo["esearchresult"]["idlist"]
            new_id = result_set[0]
            _update_new_id = new_id
            alert_name = False
            f = open("/Users/mkorovkin/Desktop/" + new_id + ".txt", "w+")
            f.write("https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esummary.fcgi?db=pubmed&id=" + new_id + "&retmode=json")
            f.close()
            yield scrapy.Request("https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esummary.fcgi?db=pubmed&id=" + new_id + "&retmode=json", callback=self.parse)
        else:
            artinfo = json.loads(response.text)
            result_set = artinfo["result"]
            found_pub_date = False

            for uid in result_set:
                # UID must be an integer
                if uid == str(search_id) or uid == str(_update_new_id):
                    if _update_new_id == "0":
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
                        link_dict[link] = -1
                    break

        # Extract publication IDs for the cited-in field (papers that cite this publication)
        for linksetdb in linkset:
            linksetdbs = linksetdb["linksetdbs"]
            for item in linksetdbs:
                print(item["linkname"])
                if item["linkname"] == "pubmed_pubmed_citedin":  # Forwards
                    print("ni")
                    found_citedin = True
                    pubmed_citedin = item["links"]
                    for link in pubmed_citedin:
                        link_dict[link] = 1
                    break

        keys = link_dict.keys()
        split = 20

        key_string_forwards = {}
        key_string_backwards = {}
        counter_forwards = 0
        counter_backwards = 0
        for key in keys:
            if link_dict[key] == -1:
                if not counter_backwards % split in key_string_backwards:
                    key_string_backwards[counter_backwards % split] = str(key) + ","
                else:
                    key_string_backwards[counter_backwards % split] += str(key) + ","
                counter_backwards += 1
            elif link_dict[key] == 1:
                if not counter_forwards % split in key_string_forwards:
                    key_string_forwards[counter_forwards % split] = str(key) + ","
                else:
                    key_string_forwards[counter_forwards % split] += str(key) + ","
                counter_forwards += 1

        for i in range(split):
            if i in key_string_backwards:
                search_string_b = key_string_backwards[i]
                yield scrapy.Request(
                    "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esummary.fcgi?db=pubmed&id=" + search_string_b + "&retmode=json",
                    callback=self.parse_base_article_string,
                    meta={
                        'layer': response.meta["layer"] + 1,
                        'parent': 0,  # response.meta["id"],
                        'fb': -1
                    }
                    )

            if i in key_string_forwards:
                search_string_f = key_string_forwards[i]
                yield scrapy.Request(
                    "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esummary.fcgi?db=pubmed&id=" + search_string_f + "&retmode=json",
                    callback=self.parse_base_article_string,
                    meta={
                        'layer': response.meta["layer"] + 1,
                        'parent': 0,  # response.meta["id"],
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

def represents_int(s):
    for i in range(10):
        if s.startswith(str(i)):
            return True
    return False

# -- -- -- -- -- -- -- -- -- -- Script initiation code -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- --

first_arg = "30013015"#"29656858"#sys.argv[1]
second_arg = "100"#sys.argv[2]
third_arg = "False"#sys.argv[3]
output_name = "ME"#sys.argv[4]

search_id = first_arg
to_id = second_arg
represents_string = "True" == third_arg

alert_name = False
alert_name_permanent = False
_update_new_id = "0"

if represents_string:
    search_id = search_id.replace("_", "+")
    alert_name = True
    alert_name_permanent = True

_pub_name = "::name not found::"
_pub_year = "::year not found::"
_pub_doi = "::doi not found::"
_pub_la = "::la not found::"
_pub_fa = "::fa not found::"
_pub_id = "::id::"
_pub_authors = list()

#search_id = 29656858

# Global dataframe which contains all data retrieved on parent and child publications
gdf = pd.DataFrame(columns=['date', 'id', 'name', 'layer', 'parent', 'fb'])

# Create a spider to perform the web crawling process
process = CrawlerProcess({
    'USER_AGENT': 'Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1)'
})

url = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esummary.fcgi?db=pubmed&id=" + str(search_id) + "&retmode=json"
if alert_name:
    url = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi?db=pubmed&term=" + search_id + "[title]&retmax=100&retmode=json"

ChamberSpider.start_urls = [ url ]
process.crawl(ChamberSpider)
x = process.start()

if alert_name_permanent:
    search_id = output_name
#
# f = open("/Users/mkorovkin/Desktop/PRINT," + first_arg + ".txt", "w+")
# f.write("alert_name:" + str(alert_name) + "\n")
# f.write("alert_name_permanent:" + str(alert_name_permanent) + "\n")
# f.write("search_id:" + str(search_id) + "\n")
# f.write("first_arg:" + str(first_arg) + "\n")
# f.write("second_arg:" + str(second_arg) + "\n")
# f.close()

# Output the contents of the dataframe into a csv file
gdf.to_csv(path_or_buf="/Users/mkorovkin/Desktop/citations_" + str(search_id) + ".csv", encoding='utf-8')

# Notify the user once data collection is finished
print("Finished data collection on the publication history of ID \"" + str(search_id) + "\"")
print("Dropped " + str(len(dropped_rows)) + " ids.")

f = open("/Users/mkorovkin/Desktop/citations_" + str(search_id) + "_info.txt","w+")
f.write("name:" + _pub_name + "\n")
f.write("year:" + _pub_year + "\n")
f.write("doi:" + _pub_doi + "\n")
f.write("id:" + _pub_id + "\n")
f.write("fa:" + _pub_fa + "\n")
f.write("la:" + _pub_la + "\n")
for _author in _pub_authors:
    f.write("gauthor:" + _author + "\n")
f.close()