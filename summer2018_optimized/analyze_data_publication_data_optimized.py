import pandas as pd
import numpy as np

#df = pd.read_csv("/Users/mkorovkin/Desktop/citations_2010_data_ms644808.csv")

def publication_score(df):
    fdf = df.loc[df['layer'] > 0]
    dgb = fdf.groupby(['id'])

    #author_names = np.array(df.loc[df['layer'] == 0]['name'])
    author_names = np.array(["liu b", "liu f", "wang x", "chen j", "fang l", "chou kc"])
    author_repeat_count = 0
    passes = 0

    for tab in dgb:
        passes += 1
        names = tab[1]['name']
        for name in names:
            if name in author_names:
                author_repeat_count += 1
                break


    num_pubs = np.float32(len(dgb.count()))

    prop = np.float32(author_repeat_count) / num_pubs

    return prop, passes

print(publication_score(pd.read_csv("/Users/mkorovkin/Desktop/citations_25958395.csv")))
#print("Self-citation proportion of publication: " + str(np.round(prop * 100.0, decimals=2)) + "% (" + str(author_repeat_count) + "/" + str(np.int32(num_pubs)) + ")")