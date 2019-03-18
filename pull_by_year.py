import scrapy
from scrapy.crawler import CrawlerProcess
from scrapy.selector import Selector
import pandas as pd
import matplotlib.pyplot as mp
import numpy as np
from unidecode import unidecode

class ChamberSpider(scrapy.Spider):
    name="chamber"
    start_urls = ["http://cm.pschamber.com/list/"]
    # iterations = 0

    def parse(self, response):
        print("Retrieved first set of articles...")

        id_list = Selector(response=response).xpath('//Id/text()').extract()
        id_list = [(x).encode('utf-8') for x in id_list]
        id_list2 = np.random.choice(id_list, 300, replace=False)

        for id in id_list2:
            print("reached list2 iteration")

            future_link = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi?db=pubmed&id=" + id + "&rettype=fasta&retmode=xml"
            ref_link = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/elink.fcgi?dbfrom=pubmed&linkname=pubmed_pmc_refs&id=" + id

            yield scrapy.Request(ref_link, callback=self.parse_2, meta={'link': future_link, 'id': id, 'layer': 0, 'parent': id})

    def parse_2(self, response):
        cited_ids = Selector(response=response).xpath('//Link/Id/text()').extract()
        has_ids = False
        if len(cited_ids):
            has_ids = True
            cited_ids = [(x).encode('utf-8') for x in cited_ids]
        else:
            print("ERROR: NO CITATIONS AVAILABLE")

        print(cited_ids)

        if has_ids:
            yield scrapy.Request(response.meta['link'], callback=self.parse_3, meta={'cited_ids': cited_ids, 'id': response.meta['id'], 'layer': response.meta['layer'], 'parent': response.meta['parent']})
        else:
            yield scrapy.Request(response.meta['link'], callback=self.parse_3_noid, meta={'cited_ids': cited_ids, 'id': response.meta['id'], 'layer': response.meta['layer'], 'parent': response.meta['parent']})

    def parse_3(self, response):
        print("Retrieved author information...")
        xml_text = response.text

        last_names = Selector(response=response).xpath('//Author/LastName/text()').extract()
        first_names = Selector(response=response).xpath('//Author/ForeName/text()').extract()
        affiliations = Selector(response=response).xpath('//Author/AffiliationInfo/Affiliation/text()').extract()
        cites = Selector(response=response).xpath('//CommentsCorrections/PMID/text()').extract()

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
        else:
            print(date)

        last_names = [(x).encode('utf-8') for x in last_names]
        first_names = [(x).encode('utf-8') for x in first_names]
        affiliations = [(x).encode('utf-8') for x in affiliations]
        cites = [(x).encode('utf-8') for x in cites]

        doi = Selector(response=response).xpath('//ELocationID/text()').extract()

        abstract = Selector(response=response).xpath('//AbstractText/text()').extract()
        if len(abstract) > 0:
            abstract = abstract[0].encode('utf-8')
        else:
            abstract = None

        print("Finished retrieving author information...")

        cited_ids = response.meta['cited_ids']

        print("Parsing article in set...")

        name_concat = list()
        for i in range(len(last_names)):
            name_concat.append(last_names[i].lower() + ", " + first_names[i].lower())

        print("NAME_CONCAT: ", name_concat)
        print("Layer: ", response.meta['layer'])

        print("- - - - - - - -")
        print(pd.DataFrame(data=[name_concat, affiliations]).transpose())

        doi = [(x).encode('utf-8') for x in doi]
        for id in doi:
            if id.find('.') > 0:
                doi = id
                break

        for name in name_concat:
            if response.meta['layer'] == 0:
                add_data(response.meta['id'], name, response.meta['layer'], date, response.meta['parent'], 0)
            else:
                if name in response.meta['authors']:
                    add_data(response.meta['id'], name, response.meta['layer'], date, response.meta['parent'], 1)
                else:
                    add_data(response.meta['id'], name, response.meta['layer'], date, response.meta['parent'], 0)


        if not response.meta['layer'] > 0:
            for id in cited_ids:
                if not checked(id):
                    register_found(id)
                    link = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/elink.fcgi?dbfrom=pubmed&linkname=pubmed_pmc_refs&id=" + id
                    future_link = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi?db=pubmed&id=" + id + "&rettype=fasta&retmode=xml"
                    print(":> id =", id)
                    print(":> parent_id =", response.meta['id'])
                    yield scrapy.Request(link, callback=self.parse_2, meta={'link': future_link, 'id': id, 'layer': response.meta['layer'] + 1, 'parent': response.meta['id'], 'authors': name_concat})
        else:
            print("Finished first layer")

    def parse_3_noid(self, response):
        print("Retrieved author information...")
        xml_text = response.text

        last_names = Selector(response=response).xpath('//Author/LastName/text()').extract()
        first_names = Selector(response=response).xpath('//Author/ForeName/text()').extract()
        affiliations = Selector(response=response).xpath('//Author/AffiliationInfo/Affiliation/text()').extract()
        cites = Selector(response=response).xpath('//CommentsCorrections/PMID/text()').extract()

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
        print(date)

        last_names = [(x).encode('utf-8') for x in last_names]
        first_names = [(x).encode('utf-8') for x in first_names]
        affiliations = [(x).encode('utf-8') for x in affiliations]
        cites = [(x).encode('utf-8') for x in cites]

        doi = Selector(response=response).xpath('//ELocationID/text()').extract()

        abstract = Selector(response=response).xpath('//AbstractText/text()').extract()
        if len(abstract) > 0:
            abstract = abstract[0].encode('utf-8')
        else:
            abstract = None

        print("Finished retrieving author information...")

        print("Parsing article in set...")

        name_concat = list()
        for i in range(len(last_names)):
            name_concat.append(last_names[i].lower() + ", " + first_names[i].lower())

        for name in name_concat:
            if response.meta['layer'] == 0:
                add_data(response.meta['id'], name, response.meta['layer'], date, response.meta['parent'], 0)
            else:
                if name in response.meta['authors']:
                    add_data(response.meta['id'], name, response.meta['layer'], date, response.meta['parent'], 1)
                else:
                    add_data(response.meta['id'], name, response.meta['layer'], date, response.meta['parent'], 0)

        doi = [(x).encode('utf-8') for x in doi]
        for id in doi:
            if id.find('.') > 0:
                doi = id
                break
        else:
            print("Finished first layer")

def add_data(id, author, layer, date, parent, selfcite):
    temp_df = pd.DataFrame(data=[id, author, layer, date, parent, selfcite]).transpose()
    temp_df.columns = ['id', 'name', 'layer', 'date', 'parent', 'selfcite']
    print("Adding new data point...")
    print(temp_df)
    global gdf
    gdf = gdf.append(temp_df)

    global coauthors
    if layer == 0:
        print("Added coauthor...")
        coauthors.append(author)
        print(coauthors)

def checked(string):
    global checked_list
    for s in checked_list:
        if s == string:
            return True
    return False

def register_found(string):
    global checked_list
    checked_list.append(string)

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

checked_list = list()
gdf = pd.DataFrame(columns=['date', 'id', 'name', 'layer', 'parent', 'selfcite'])
coauthors = list()

process = CrawlerProcess({
    'USER_AGENT': 'Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1)'
})

# 2015
ChamberSpider.start_urls = ["https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi?db=pubmed&term=2013[PDat]&RetMax=1000000"]

process.crawl(ChamberSpider)
x = process.start()

print(gdf)

gdf.to_csv(path_or_buf="/Users/mkorovkin/Desktop/citations_2013_data.csv")