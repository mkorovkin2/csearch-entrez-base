import pandas as pd
import numpy as np
import matplotlib.pyplot as mp

def find_self_citation_percentage_from_id(df, id):
    cited_by = df.loc[df['parent'] == id][['id', 'name', 'layer']]
    cited_by = cited_by.loc[cited_by['layer'] > 0]
    authors_of_id = df.loc[df['id'] == id][['name']]

    list_found = list()
    for x in range(len(authors_of_id)):
        n = authors_of_id.iloc[x]['name']
        cc = cited_by.loc[cited_by['name'] == n]
        for two in range(len(cc)):
            nname = cc.iloc[two]['name']
            list_found.append(nname)
    len3 = len(cited_by.groupby('id').count().reset_index())
    if (len3 > 0):
        prop = np.float32(len(list_found)) / np.float32(len3)
        #print(prop)
        if prop > 1:
            return 0.0
        else:
            if prop > 0.18:
                print("Id \"id_" + str(id) + "\" returned a self-citation proportion of " + str(prop))
            return prop
    else:
        return 0.0

df = pd.read_csv("/Users/mkorovkin/Desktop/citations_2013_data_ms421180.csv")#citations_2015_data_ms93357.csv")#citations_2013_data_ms903668.csv")

parents = df.loc[df['layer'] == 0]
gb = parents.groupby('id').count().reset_index()

prop_series = pd.Series()
count = 0.0
for id in range(len(gb[['id']])):
    idz = gb.iloc[id]['id']
    df_test = df.loc[df['parent'] == idz]
    df_test = df_test.loc[df_test['layer'] > 0]
    if len(df_test) > 0:
        prop_series = prop_series.append(pd.Series([find_self_citation_percentage_from_id(df, idz)]))

print("\n- - - - - - -")

print("\nMean: " + str(prop_series.mean()))
print("Std.dev: " + str(prop_series.std()))
#print("\nMax: " + str(prop_series.max()))
print("N: " + str(len(prop_series)))

n, bins, patches = mp.hist(prop_series, 100, facecolor='blue', alpha=0.75)
mp.xlabel('Proportion of self-citations')
mp.ylabel('Frequency')
mp.title(r'$\mathrm{Histogram\ of\ IQ:}\ \mu=' + str(np.round(prop_series.mean(), decimals=2)) + ',\ \sigma=' + str(np.round(prop_series.std(), decimals=2)) + '$')
mp.axis([0, 1, 0, 80])
mp.grid(True)
mp.axvline(prop_series.mean(), color='red', linestyle='dashed', linewidth=1)

mp.show()

# given an author, though....
namet = 'besch, dorothea'
authorin_df = df.loc[df['name'] == namet]
authorin_df = authorin_df.loc[df['layer'] == 0]
pid = authorin_df.iloc[0]['id']

not_layer = df.loc[df['layer'] > 0]
not_layer = not_layer.loc[df['parent'] == pid]
pcc = np.float32(len(df.loc[df['name'] == namet])) / np.float32(len(not_layer.groupby('id').count().reset_index()))

print(namet + " appears in " + str(np.round(pcc * 100.0, decimals=2)) + " of the author space of their paper's citations.")