import scrapy
from scrapy.crawler import CrawlerProcess
from scrapy.selector import Selector
import pandas as pd
import numpy as np
import datetime

class ChamberSpider(scrapy.Spider):
    name="chamber"
    start_urls = ["http://cm.pschamber.com/list/"]

    def parse(self, response):
        print("Retrieved first set of articles...")

        # Extract ids of
        id_list = Selector(response=response).xpath('//Id/text()').extract()
        id_list = np.random.choice([(x).encode('utf-8') for x in id_list], 100000, replace=False)

        print(str(len(id_list)) + " articles found.")

        # Extract ids from a list and repeat this operation for all of them
        # Retreives list of the paper's references and articles that cite the paper in their references
        for id in id_list:
            # https://eutils.ncbi.nlm.nih.gov/entrez/eutils/elink.fcgi?dbfrom=pubmed&linkname=pubmed_pmc_refs&id=
            article_information_link = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi?db=pubmed&id=" + id + "&rettype=fasta&retmode=xml"
            #references_link = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/elink.fcgi?dbfrom=pubmed&id=" + id
            references_link = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/elink.fcgi?dbfrom=pubmed&linkname=pubmed_pmc_refs&id=" + id
            yield scrapy.Request(references_link, callback=self.parse_parent_id, meta={'link': article_information_link, 'id': id, 'layer': 0, 'parent': id})

    def parse_parent_id(self, response):
        cited_ids = Selector(response=response).xpath('//LinkSetDb/Link/Id/text()').extract()
        cited_ids = [(x).encode('utf-8') for x in cited_ids]

        if not len(cited_ids):
            print("ERROR IN CITATION: no ids found for article", response.meta['id'])
            return

        yield scrapy.Request(response.meta['link'], callback=self.parse_individual_article, meta={'cited_ids': cited_ids, 'id': response.meta['id'], 'layer': response.meta['layer'], 'parent': response.meta['parent']})

    def parse_individual_article(self, response):
        print("Retrieved author information...")
        xml_text = response.text

        # Retrieve data for each article; retrieve affiliations if available
        last_names = Selector(response=response).xpath('//Author/LastName/text()').extract()
        first_names = Selector(response=response).xpath('//Author/ForeName/text()').extract()
        affiliations = Selector(response=response).xpath('//Author/AffiliationInfo/Affiliation/text()').extract()
        cites = Selector(response=response).xpath('//CommentsCorrections/PMID/text()').extract()

        last_names = [(x).encode('utf-8') for x in last_names]
        first_names = [(x).encode('utf-8') for x in first_names]
        affiliations = [(x).encode('utf-8') for x in affiliations]
        cites = [(x).encode('utf-8') for x in cites]

        full_names = list()
        for i in range(len(last_names)):
            full_names.append(last_names[i].lower() + ", " + first_names[i].lower())

        # Retrieve the article's date
        date = ""
        date_year = Selector(response=response).xpath('//DateCompleted/Year/text()').extract()
        if (len(date_year) > 0):
            date_year = date_year[0].encode('utf-8')
            date = date + date_year
        date = date + "-"
        date_month = Selector(response=response).xpath('//DateCompleted/Month/text()').extract()
        if (len(date_month) > 0):
            date_month = date_month[0].encode('utf-8')
            date = date + date_month
        date_day = Selector(response=response).xpath('//DateCompleted/Day/text()').extract()
        date = date + "-"
        if (len(date_day) > 0):
            date_day = date_day[0].encode('utf-8')
            date = date + date_day
        if date == "--":
            return

        global year
        if int(date[:date.find("-")]) > year:
            return
        else:
            for name in full_names:
                add_data(response.meta['id'], name, response.meta['layer'], date, response.meta['parent'])

        cited_ids = response.meta['cited_ids']

        if not response.meta['layer'] > 0:
            for id in cited_ids:
                if not checked(id):
                    register_found(id)
                    #article_information_link = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/elink.fcgi?dbfrom=pubmed&linkname=pubmed_pmc_refs&id=" + id
                    #references_link = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi?db=pubmed&id=" + id + "&rettype=fasta&retmode=xml"
                    article_information_link = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi?db=pubmed&id=" + id + "&rettype=fasta&retmode=xml"
                    #references_link = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/elink.fcgi?dbfrom=pubmed&id=" + id
                    references_link = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/elink.fcgi?dbfrom=pubmed&linkname=pubmed_pmc_refs&id=" + id
                    yield scrapy.Request(references_link, callback=self.parse_parent_id,
                                         meta={'link': article_information_link, 'id': id, 'layer': response.meta['layer'] + 1,
                                               'parent': response.meta['id'], 'authors': full_names})
        else:
            print("Finished layer")
        yield

# -- -- -- -- -- -- -- -- -- -- Helper methods for data handling -- -- -- -- -- -- -- -- -- --

# Adds data to the overall dataframe
def add_data(id, author, layer, date, parent):
    temp_df = pd.DataFrame(data=[id, author, layer, date, parent]).transpose()
    temp_df.columns = ['id', 'name', 'layer', 'date', 'parent']
    print("Adding new data point...")

    global gdf
    gdf = gdf.append(temp_df)

# Keeps track of article IDs that have been accessed to improve efficiency
def checked(string):
    global checked_list
    for s in checked_list:
        if s == string:
            return True
    return False

# Registers a given article ID found to improve efficiency
def register_found(string):
    global checked_list
    checked_list.append(string)

# -- -- -- -- -- -- -- -- -- -- Script initiation code -- -- -- -- -- -- -- -- -- --

# Initializing global elements
year = 2015
checked_list = list()
gdf = pd.DataFrame(columns=['date', 'id', 'name', 'layer', 'parent'])

# Create a crawler process
process = CrawlerProcess({
    'USER_AGENT': 'Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1)'
})
ChamberSpider.start_urls = ["https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi?db=pubmed&term=" + str(year) + "[PDat]&RetMax=10000000"]
process.crawl(ChamberSpider)
x = process.start()

# Output the contents of the dataframe into a csv file
gdf.to_csv(path_or_buf="/Users/mkorovkin/Desktop/citations_" + str(year) + "_data_ms" + str(datetime.datetime.now().microsecond) + ".csv")