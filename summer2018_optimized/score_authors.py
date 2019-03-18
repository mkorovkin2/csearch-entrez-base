import pandas as pd
import unidecode
from os import system
import summer2018_optimized.pull_data_Entrez_optimized_individual3 as pull
from threading import Thread

def scrape_authors(path):
    dataframe = pd.read_csv(path)

    gb = dataframe.groupby(['id'])
    chainlist = list()

    offset = 20
    count = 0

    for table in gb:
        name = unidecode.unidecode(list(table[1]['name'])[0])
        search_name_nospace = name.replace(" ", "+")
        to_id = name.replace(" ", "_")
        system("python /Users/mkorovkin/Desktop/zigzag.py " + search_name_nospace + " " + to_id + " False")
        if len(chainlist) > 50:
            break
        count += 1

scrape_authors("/Users/mkorovkin/Desktop/citations_2010_data_ms392903.csv")