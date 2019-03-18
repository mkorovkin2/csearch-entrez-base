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
        # Select a random set of papers from the link body
        id_list = Selector(response=response).xpath('//Id/text()').extract()
        id_list = np.random.choice([(x).encode('utf-8') for x in id_list], 5000, replace=False)

        # Print an update
        print("Retrieved randomized set of articles...")

        # Extract ids from a list and call two spiders for each
        for id in id_list:
            id = np.int32(np.asscalar(np.float32(id)))
            # Construct URLs for the spiders
            article_information_link = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esummary.fcgi?db=pubmed&id=" + str(id) + "&retmode=json"
            references_link = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/elink.fcgi?dbfrom=pubmed&id=" + str(id) + "&retmode=json"
            # Call the spiders
            yield scrapy.Request(article_information_link,
                                 callback=self.parse_base_article,
                                 meta={
                                     'id': id,
                                     'layer': 0,
                                     'parent': id,
                                     'fb': 0
                                 }
                                 )
            yield scrapy.Request(references_link,
                                 callback=self.parse_parent_id,
                                 meta={
                                     'link': article_information_link,
                                     'id': id,
                                     'layer': 0,
                                     'parent': id,
                                     'fb': 0
                                 }
                                 )

    # Method called back once each article_information_link is parsed
    def parse_base_article(self, response):
        author_list = list()

        # Parse the JSON response
        pp_refs = json.loads(response.text)
        result_set = pp_refs["result"]

        # Extract author/co-author names
        if "authors" in result_set[str(response.meta["id"])]:
            for sub in result_set[str(response.meta["id"])]["authors"]:
                author_list.append(sub["name"])

            # Extract the publication's date
            date = result_set[str(response.meta["id"])]["pubdate"]

            # Add an entry to the overall dataframe "gdf" for each author
            for author_ in author_list:
                add_data(response.meta["id"], author_, response.meta["layer"], date, response.meta["parent"],
                         response.meta["fb"])
        else:
            drop_rows(response.meta["id"])

    # Method called back once each references_link is parsed
    def parse_parent_id(self, response):
        # Parse JSON
        pp_refs = json.loads(response.text)
        linkset = pp_refs["linksets"]

        link_dict = {}

        found_refs = False
        found_citedin = False

        # Extract publication IDs for the references (papers cited by this publication)
        for linksetdb in linkset:
            linksetdbs = linksetdb["linksetdbs"]
            found_linkname = False
            for item in linksetdbs:
                if item["linkname"] == "pubmed_pubmed_refs": # Backwards
                    pubmed_refs = item["links"]
                    found_refs = True
                    for link in pubmed_refs:
                        indiv_id = np.int32(np.asscalar(np.float32(link)))
                        link_dict[indiv_id ] = -1
                    found_linkname = True
                    break
            if not found_linkname:
                drop_rows(response.meta["id"])
                return

        # Extract publication IDs for the cited-in field (papers that cite this publication)
        for linksetdb in linkset:
            linksetdbs = linksetdb["linksetdbs"]
            for item in linksetdbs:
                if item["linkname"] == "pubmed_pubmed_citedin": # Forwards
                    pubmed_citedin = item["links"]
                    found_refs = True
                    for link in pubmed_citedin:
                        indiv_id = np.int32(np.asscalar(np.float32(link)))
                        link_dict[indiv_id] = 1
                    break

        # All following opporations are performed using dictionary "link_dict"
        keys = link_dict.keys()

        # Drop the publication from the dataframe if it has no listed references or cited-in
        if not found_refs: # and not found_citedin:
            drop_rows(response.meta["id"])
            return

        # Perform a recursive function call of the same nature as found in "parse"
        for key in keys:
            # Retrieve information on all references with IDs found and add them to a dataframe
            article_information_link = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esummary.fcgi?db=pubmed&id=" + str(key) + "&retmode=json"
            yield scrapy.Request(article_information_link,
                                 callback=self.parse_base_article,
                                 meta={
                                     'id': key,
                                     'layer': response.meta["layer"] + 1,
                                     'parent': response.meta["id"],
                                     'fb': link_dict[key]
                                 }
                                 )
            # Retrieve citations for each IDs only if the current layer is below the value stored at layers_to_stop
            if response.meta["layer"] < self.layer_to_stop - 1:
                references_link = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/elink.fcgi?dbfrom=pubmed&id=" + str(key) + "&retmode=json"
                yield scrapy.Request(references_link,
                                     callback=self.parse_parent_id,
                                     meta={
                                         'link': article_information_link,
                                         'id': key,
                                         'layer': response.meta["layer"] + 1,
                                         'parent': response.meta["id"],
                                         'fb': link_dict[key]
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
year = 2010

# Global dataframe storing the data retrieved by this script
gdf = pd.DataFrame(columns=['date', 'id', 'name', 'layer', 'parent', 'fb'])

# Create a crawler process to manage the web scraping in the script
process = CrawlerProcess({
    'USER_AGENT': 'Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1)'
})
ChamberSpider.start_urls = ["https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi?db=pubmed&term=" + str(year) + "[PDat]&RetMax=1000000"]
process.crawl(ChamberSpider)
x = process.start()

# Output the contents of the dataframe into a csv file
gdf.to_csv(path_or_buf="/Users/mkorovkin/Desktop/citations_" + str(year) + "_data_ms" + str(datetime.datetime.now().microsecond) + ".csv", encoding='utf-8')

# Update the user on the program's status
print("Program completed.")
print("Dropped " + str(len(dropped_rows)) + " ids.")