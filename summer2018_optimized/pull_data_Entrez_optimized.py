import scrapy
from scrapy.crawler import CrawlerProcess
from scrapy.selector import Selector
import pandas as pd
import numpy as np
import datetime
import json

class ChamberSpider(scrapy.Spider):
    name="chamber"
    start_urls = ["http://cm.pschamber.com/list/"]
    layer_to_stop = 1

    # The first method called when the ChamberSpider reaches its link
    def parse(self, response):
        global to_choose

        # Select a random set of papers from the link body
        id_list = Selector(response=response).xpath('//Id/text()').extract()
        id_list = np.random.choice([(x).encode('utf-8') for x in id_list], to_choose, replace=False)

        # Print an update
        print("Retrieved randomized set of articles...")

        # Construct a string of IDs
        id_string = ""
        id_list_int = list()
        id_list_strings = list()
        for id in id_list:
            indiv_id = np.int32(np.asscalar(np.float32(id)))
            id_list_int.append(indiv_id)
            string_to_append = (u'%d' % indiv_id)
            id_list_strings.append(string_to_append)

        for ix in range(len(id_list_strings)):
            if ix != 0 and ix % 15 == 0:
                # Construct URLs for the spiders
                article_information_link = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esummary.fcgi?db=pubmed&id=" + id_string + "&retmode=json"

                # Call the spiders
                yield scrapy.Request(article_information_link,
                                     callback=self.parse_base_article_string,
                                     meta={
                                         'id': id_list_int,
                                         'layer': 0,
                                         'fb': 0
                                     }
                                     )

                for id_string_indiv in id_list_strings:
                    references_link = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/elink.fcgi?dbfrom=pubmed&id=" + id_string + "&retmode=json"
                    yield scrapy.Request(references_link,
                                         callback=self.parse_parent_id,
                                         meta={
                                             'id': id_string_indiv,
                                             'layer': 0,
                                             'parent': id_string_indiv,
                                             'fb': 0
                                         }
                                         )
                print(id_string)
                id_string = ""
            else:
                id_string += id_list_strings[ix] + ","

    # Method called back once each article_information_link is parsed
    def parse_base_article_string(self, response):
        # Parse the JSON response
        pp_refs = json.loads(response.text)
        if response.meta['layer'] == 0:
            print("LAYER == 0")
        if "result" in pp_refs:
            result_set = pp_refs["result"]

            # Find all authors of each UID
            for uid in result_set:
                # UID must be an integer
                if uid != "uids":
                    df_uid = uid
                    df_date = "not_found"
                    if "pubdate" in result_set[str(uid)]:
                        df_date = result_set[str(uid)]["pubdate"]
                    if "authors" in result_set[str(uid)]:
                        for author_element in result_set[str(uid)]["authors"]:
                            df_author = author_element["name"]
                            # Add an entry to the dataframe for each author
                            if response.meta["layer"] == 0:
                                add_data(df_uid, df_author, response.meta["layer"], df_date, df_uid, response.meta["fb"])
                            else:
                                add_data(df_uid, df_author, response.meta["layer"], df_date, response.meta["parent"], response.meta["fb"])
                    else:
                        drop_rows(uid)

    def parse_base_article(self, response):
        # Parse the JSON response
        pp_refs = json.loads(response.text)
        result_set = pp_refs["result"]

        for element in result_set:
            if str(element) == str(result_set['uids'][0]):
                df_date = result_set[element]['pubdate']
                for author_elem in result_set[element]['authors']:
                    df_author_name = author_elem['name']
                    add_data(element, df_author_name, response.meta["layer"], df_date, response.meta["parent"], response.meta["fb"])


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
                    found_refs = True
                    for link in pubmed_refs:
                        link_dict[link] = -1
                    break

        # Extract publication IDs for the cited-in field (papers that cite this publication)
        for linksetdb in linkset:
            linksetdbs = linksetdb["linksetdbs"]
            for item in linksetdbs:
                if item["linkname"] == "pubmed_pubmed_citedin":  # Forwards
                    pubmed_citedin = item["links"]
                    for link in pubmed_citedin:
                        link_dict[link] = 1
                    break

        # All following opporations are performed using dictionary "link_dict"
        keys = link_dict.keys()

        # Drop the publication from the dataframe if it has no listed references or cited-in
        if not found_refs:
            drop_rows(response.meta["id"])
            return

        # Perform a recursive function call of the same nature as found in "parse"
        for key in keys:
            # Retrieve information on all references with IDs found and add them to a dataframe
            article_information_link = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esummary.fcgi?db=pubmed&id=" + str(
                key) + "&retmode=json"
            yield scrapy.Request(article_information_link,
                                 callback=self.parse_base_article,
                                 meta={
                                     'id': key,
                                     'layer': response.meta["layer"] + 1,
                                     'parent': 0, # response.meta["id"],
                                     'fb': link_dict[key]
                                 }
                                 )
            # Retrieve citations for each IDs only if the current layer is below the value stored at layers_to_stop
            if response.meta["layer"] < self.layer_to_stop - 1:
                references_link = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/elink.fcgi?dbfrom=pubmed&id=" + str(
                    key) + "&retmode=json"
                yield scrapy.Request(references_link,
                                     callback=self.parse_parent_id,
                                     meta={
                                         'link': article_information_link,
                                         'id': key,
                                         'layer': response.meta["layer"] + 1,
                                         'parent': 0, # response.meta["id"],
                                         'fb': link_dict[key]
                                     }
                                     )

    # Method called back once each references_link is parsed
    def parse_parent_id(self, response):
        global to_choose

        # Parse JSON
        pp_refs = json.loads(response.text)
        linkset = pp_refs["linksets"]

        link_string_forwards = ""
        link_string_backwards = ""

        found_refs = False
        found_citedin = False

        # Extract publication IDs for the references (papers cited by this publication)
        for linksetdb in linkset:
            linksetdbs = linksetdb["linksetdbs"]
            for item in linksetdbs:
                if item["linkname"] == "pubmed_pubmed_refs": # Backwards
                    pubmed_refs = item["links"]
                    found_refs = True
                    for link in pubmed_refs:
                        indiv_id = np.asscalar(np.float32(link))
                        string_to_append = (u'%d' % indiv_id)
                        link_string_backwards += string_to_append + ','
                    break

        # Extract publication IDs for the cited-in field (papers that cite this publication)
        for linksetdb in linkset:
            linksetdbs = linksetdb["linksetdbs"]
            for item in linksetdbs:
                if item["linkname"] == "pubmed_pubmed_citedin": # Forwards
                    found_citedin = True
                    pubmed_citedin = item["links"]
                    for link in pubmed_citedin:
                        indiv_id = np.asscalar(np.float32(link))
                        string_to_append = (u'%d' % indiv_id)
                        link_string_forwards += string_to_append + ','
                    break

        link_string_backwards = link_string_backwards[:(len(link_string_backwards) - 1)]
        link_string_forwards = link_string_forwards[:(len(link_string_forwards) - 1)]

        # Drop the publication from the dataframe if it has no listed references or cited-in
        if not found_refs and not found_citedin:
            drop_rows(response.meta["id"])
            return

        article_information_link_forwards = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esummary.fcgi?db=pubmed&id=" + link_string_forwards + "&retmode=json"
        yield scrapy.Request(article_information_link_forwards,
                             callback=self.parse_base_article_string,
                             meta={
                                 'layer': response.meta["layer"] + 1,
                                 'parent': response.meta["parent"],
                                 'fb': 1
                             }
                             )
        article_information_link_backward = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esummary.fcgi?db=pubmed&id=" + link_string_backwards + "&retmode=json"
        yield scrapy.Request(article_information_link_backward,
                             callback=self.parse_base_article_string,
                             meta={
                                 'layer': response.meta["layer"] + 1,
                                 'parent': response.meta["parent"],
                                 'fb': -1
                             }
                             )

# -- -- -- -- -- -- -- -- -- -- Helper methods for data handling -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -

# List that keeps track of which rows were dropped in handling data
dropped_rows = list()

# Adds data to the overall dataframe
def add_data(id, author, layer, date, parent, f_b):
    # Make author a lower case string to keep consistency in naming and create robustness in name matching
    author = author.lower()

    # Manipulate the dataframe to store the data taken in by the method
    temp_df = pd.DataFrame(data=[date, id, author, layer, parent, f_b]).transpose()
    temp_df.columns = ['date', 'id', 'name', 'layer', 'parent', 'fb']

    # Append the current dataframe to the global dataframe
    global gdf
    gdf = gdf.append(temp_df)

    # print("Added new data point [" + str(id) + "; layer = " + str(layer) + "]")

# Drop a set of rows given a publication ID
def drop_rows(id):
    dropped_rows.append(id)
    global gdf
    gdf = gdf[gdf.id != id]

# -- -- -- -- -- -- -- -- -- -- Script initiation code -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- --

# Initializing global elements
to_choose = 1000
year = 2007

# Global dataframe storing the data retrieved by this script
gdf = pd.DataFrame(columns=['date', 'id', 'name', 'layer', 'parent', 'fb'])

# Create a crawler process to manage the web scraping in the script
process = CrawlerProcess({
    'USER_AGENT': 'Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1)'
})
ChamberSpider.start_urls = ["https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi?db=pubmed&term=" + str(year) + "[PDat]&RetMax=100000"]
process.crawl(ChamberSpider)
x = process.start()

# Output the contents of the dataframe into a csv file
gdf.to_csv(path_or_buf="/Users/mkorovkin/Desktop/citations_" + str(year) + "_data_ms" + str(datetime.datetime.now().microsecond) + ".csv", encoding='utf-8')

# Update the user on the program's status
print("Program completed.")
print("Dropped " + str(len(dropped_rows)) + " ids.")