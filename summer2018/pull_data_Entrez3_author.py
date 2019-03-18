from summer2018 import pull_data_Entrez2 as pull
import pandas as pd
import random
import numpy as np

def find_self_citation_percentage_from_author(author, df, condition='backwards'):
    lim_break = 8

    ref_df = df.loc[df['parent'] == 0]
    author_count = 0
    ref_df_grouped = ref_df.groupby(['id'])
    tab_list = [tab[1]['name'] for tab in ref_df_grouped]
    for tab in tab_list:
        for name in tab:
            ln = len(name)
            if (ln > lim_break and author in name) or (ln <= lim_break and name == author):
                author_count += 1

    #print(author_count)
    #print(len(ref_df_grouped.count()))

    nn = np.float32(author_count) / np.float32(len(ref_df_grouped.count()))
    if not np.isnan(nn):
        return nn, author_count, len(ref_df_grouped)
    else:
        return 0, 1, len(ref_df_grouped)

df = pd.read_csv("/Users/mkorovkin/Desktop/citations_2010_data_ms925687.csv")

#author_list = list(df.loc[0:len(df)]['name'])
#author_sample = random.sample(author_list, 50)
#print(author_sample)

#for i in range(len(author_sample)):
#    pull.run(author_sample[i], str(i))

listt = ['wilson jf', 'kinzenbaw da', 'crispo a', 'asano s', 'bertozzi cr', 'jia w', 'alolayan y', 'mcnaughton bl', 'malenka rc', 'mednick s', 'baylin sb', 'graciarena m', 'silveira c', 'crouch e', 'yamamoto m', 'racoma io', 'apse p', 'matson c', 'burgess rw', 'adachi k', 'watkins pa', 'wood d', 'carey te', 'kowalczykowski sc', 'broderick p', 'van der aa mn', 'kearns am', 'tran a', 'mackenzie ce', 'prager r', 'chen l', 'kassab se', 'pounds j', 'barker gc', 'press ow', 'tutt an']
i = 0
pull.run(listt[i], str(i) + "yeet")

path = "/Users/mkorovkin/Desktop/citations_" + str(i) + "yeet.csv"
rate, count, length = find_self_citation_percentage_from_author(listt[i], pd.read_csv(path))
print(rate, count, length)