import scrapy
from scrapy.crawler import CrawlerProcess
import pandas as pd
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
        publication_list = list()
        for item in link_set:
            publication_list.append(item)

        # Use all extracted IDs as "parent IDs" and build a dataframe around them and their references/citations
        for id in publication_list:
            # Submit web crawling requests for each citation
            article_information_link = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esummary.fcgi?db=pubmed&id=" + str(id) + "&retmode=json"
            references_link = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/elink.fcgi?dbfrom=pubmed&id=" + str(id) + "&retmode=json"
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

    # Parse a given publication's "header information" such as author and date
    def parse_base_article(self, response):
        author_list = list()

        # Extract names from JSON response text
        pp_refs = json.loads(response.text)
        result_set = pp_refs["result"]
        for sub in result_set[str(response.meta["id"])]["authors"]:
            author_list.append(sub["name"])

        # Extract the date of an publication
        date = result_set[str(response.meta["id"])]["pubdate"]

        # Append publication information for each author name found
        for author_ in author_list:
            add_data(response.meta["id"], author_, response.meta["layer"], date, response.meta["parent"], response.meta["fb"])

    # Parse a given publication's citation information including backwards- and forwards-in-time references
    def parse_parent_id(self, response):
        # Extract JSON text
        pp_refs = json.loads(response.text)
        linkset = pp_refs["linksets"]

        link_dict = {}

        # Tracker variables that indicate whether to drop a certain citation from the dataframe (if no information is found for it)
        found_refs = False
        found_citedin = False

        # Extract PubMed IDs from JSON format - for backwards-in-time citations
        for linksetdb in linkset:
            linksetdbs = linksetdb["linksetdbs"]
            for item in linksetdbs:
                if item["linkname"] == "pubmed_pubmed_refs":
                    # Append links to the link_dict dictionary
                    pubmed_refs = item["links"]
                    # Update tracker variable to avoid dropping the parent publication
                    found_refs = True
                    # Denote all backwards-in-time citations as { "fb": 1 }
                    for link in pubmed_refs:
                        link_dict[link] = -1
                    break

        # Extract PubMed IDs from JSON format - for backwards-in-time citations
        for linksetdb in linkset:
            linksetdbs = linksetdb["linksetdbs"]
            for item in linksetdbs:
                if item["linkname"] == "pubmed_pubmed_citedin":
                    # Append links to the link_dict dictionary
                    pubmed_citedin = item["links"]
                    # Update tracker variable to avoid dropping the parent publication
                    found_citedin = True
                    # Denote all forwards-in-time citations as { "fb": 1 }
                    for link in pubmed_citedin:
                        link_dict[link] = 1
                    break

        # Use the link_dict to perform all other operations
        keys = link_dict.keys()

        # Drop citation if neither of backwards- or forwards-in-time citations were found
        if not found_refs and not found_citedin:
            drop_rows(response.meta["id"])
            return

        # For each key (citation) found in the link_dict dictionary perform recursive calls on refernces to recieve information on them
        for key in keys:
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
            # If required, recursively find references of references for scalability (not applicable if self.layers_to_stop < 2)
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
    print("Added new data point [" + str(id) + "; layer = " + str(layer) + "]")

# Drop a given ID's rows from a dataframe
def drop_rows(id):
    dropped_rows.append(id)
    global gdf
    gdf = gdf[gdf.id != id]

# Format a search term (replaces all spaces with "%20")
def format_search(word):
    return word.replace(" ", "%20")

# -- -- -- -- -- -- -- -- -- -- Script initiation code -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- --
# Take arguments from the user (1. author name, 2. gdf saving ID)
first_arg = sys.argv[1]
second_arg = sys.argv[2]

search_name = first_arg
to_id = second_arg

# Maximum number of citations to investigate
retmax = 100

# Global dataframe which contains all data retrieved on parent and child publications
gdf = pd.DataFrame(columns=['date', 'id', 'name', 'layer', 'parent', 'fb'])

# Create a spider to perform the web crawling process
process = CrawlerProcess({
    'USER_AGENT': 'Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1)'
})

ChamberSpider.start_urls = ["https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi?db=pubmed&term=" + format_search(search_name) + "[author]&retmax=" + str(retmax) + "&retmode=json"]
process.crawl(ChamberSpider)
x = process.start()

# Output the contents of the dataframe into a csv file
gdf.to_csv(path_or_buf="/Users/mkorovkin/Desktop/citations_" + to_id + ".csv", encoding='utf-8')

# Notify the user once data collection is finished
print("Finished data collection on the publication history of author \"" + search_name + "\"")
print("Dropped " + str(len(dropped_rows)) + " ids.")