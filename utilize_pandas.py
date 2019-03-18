import numpy as np
import pandas as pd

###########################################
### Not finalized code; messy           ###
### Meant to be a testing ground for    ###
### potential approaches/database code  ###
###########################################

###########################################
### Can do:                             ###
### *   Determining whether an author of###
###     a given publication is also an  ###
###     author of the parent publication###
### *   Statistical percentage of self- ###
###     citation appearances            ###
### *   Average number of self-citations###
###     per author of the chosen parent ###
###     publications                    ###
### *   Determine the number and list of###
###     publications that cite a given  ###
###     publication                     ###
### *   Determine how many publications ###
###     there are by the same author    ###
### *   Determine how many papers or    ###
###     authors cited a certain         ###
###     publication                     ###
### *   Quickly retrieve publications   ###
###     based on authors and vice versa ###
###########################################


# Pandas summary
# REport information on ID (how many citations/ how many are self-citations / authors / title / year)
    # Query paper
    # "Header information"
# Analysis
    # For distribution

def print_groupby(groupby):
    for key, item in groupby:
        print groupby.get_group(key), "\n\n"
        print("- - - key: " + str(key))

def groupby_to_list(groupby):
    list1 = list()
    for key, item in groupby:
        list1.append(groupby.get_group(key))

    return list1

df = pd.read_csv("/Users/mkorovkin/Desktop/citations_2013_data_ms764170.csv")

parent_rows = df.loc[df['parent'] == df['id']]

parent_papers = df.groupby(by=['parent', 'id'])[['name']]
parent_paper_ids = df.groupby(by=['id'])[['id']]

unique_parent_ids = pd.DataFrame({'count': df.groupby(['id', 'parent']).size()}).reset_index()['parent'].unique()
unique_paper_ids = pd.DataFrame({'count': df.groupby(['id', 'parent']).size()}).reset_index()['id'].unique()

#print(unique_parent_ids)

# How many by same author:

author = 'gregson, simon'
id_to_find = 21164081

appearances_of_author = df.loc[df['name'] == author]
author_of = df.loc[df['id'] == id_to_find]
parent_id_to_find = author_of.iloc[0]['parent']
containsz_df = df.loc[df['id'] == parent_id_to_find]['name']
containsz = author in list(containsz_df.loc[containsz_df == author])
#date_of = df.loc[df['id'] == id_to_find]['date'].unique()

#print(appearances_of_author)
#print("\nAuthor (\"" + author + "\", id$" + str(id_to_find) + ") is also a co-author of the parent citation (id$" + str(parent_id_to_find) + "): [" + str(containsz) + "]")

total = 0

authors_to_verify = list()
for paper in unique_parent_ids:
    locc = df.loc[df['id'] == paper][['name']]
    for author in locc['name']:
        authors_to_verify.append(author)

for author in authors_to_verify:
    appearances = df.loc[df['name'] == author][['layer']]
    la = len(appearances)
    #print("{:6}:: {:10}".format(str(la), author))
    total += la

size = np.float32(len(df[['id']]))
total2 = np.float32(total)
atv = np.float32(len(authors_to_verify))
#print(authors_to_verify)
#print("Percentage of self-citation = " + str(np.round((total2 - atv) / size * np.float32(100.0), decimals=2)) + "%")
#print("Average number of non-zero-layer self-citations per author = " + str(np.round((total2 - atv) / atv, decimals=2)))

id_to_find2 = 23561027

paper_cited_by = df.loc[df['parent'] == id_to_find2][['layer', 'id']]
paper_cited_by_gb = paper_cited_by[paper_cited_by['layer'] > 0].groupby(['id']).count().reset_index()
#print("Publication id$" + str(id_to_find2) + " cited by " + str(len(paper_cited_by_gb)) + " other publications.")

#print_groupby(parent_paper_ids)
#print(unique_parent_ids)

#key = (26574525, <id>)


#################################################################################################################
# Given a paper, find the number of citations and number of self-citations based on each of the authors         #
#################################################################################################################
def find_self_citation_percentage_from_id(df, id):
    cited_by = df.loc[df['parent'] == id][['id', 'name', 'layer']]
    cited_by = cited_by.loc[cited_by['layer'] > 0]
    authors_of_id = df.loc[df['id'] == id][['name']]

    print("Inspecting self-citation history for publication \"id_" + str(id) + "\"...")

    list_found = list()
    for x in range(len(authors_of_id)):
        n = authors_of_id.iloc[x]['name']
        cc = cited_by.loc[cited_by['name'] == n]
        for two in range(len(cc)):
            nname = cc.iloc[two]['name']
            list_found.append(nname)
            print(">   Found self citation of author \"" + nname + "\"")

    print("\n" + str(len(cited_by.groupby(
        'id').count().reset_index())) + " publications were found to contain at least one of the authors of the publication \"id_" + str(
        id) + "\"")
    prop = np.float32(len(list_found)) / np.float32(len(cited_by.groupby('id').count().reset_index()))
    print("Percentage of self-citation for publication \"id_" + str(id) + "\": " + str(np.round(
        np.float32(100.0 * prop),
        decimals=2)) + "%")
    return prop

df = pd.read_csv("/Users/mkorovkin/Desktop/citations_2013_data_ms421180.csv")

while (True):
    id = input("Enter a paper id (ex. 23427175): ")
    if id == 0 or id == "":
        break
    else:
        id = int(id)

        #23427175#23369936#23427175#23427175 # 23561027
        #23427175

    prop = find_self_citation_percentage_from_id(df, id)

#################################################################################################################
def find_self_citation_percentage_from_author(df, author):
    author_appears = df.loc[df['name'] == author][['id', 'layer']]
    highest_layer = 0
    lowest_layer = 1000
    for occ in range(len(author_appears)):
        ap = author_appears.iloc[occ]['layer']
        if ap > highest_layer:
            highest_layer = ap
        if ap < lowest_layer:
            lowest_layer = ap

    lowest_author = author_appears.loc[author_appears['layer'] == lowest_layer][['id', 'layer']]

    perc_sum = 0.0
    pp = 0.0
    lenn = len(lowest_author)
    for i in range(lenn):
        if lowest_author.iloc[i]['layer'] < highest_layer:
            perc = author_find(lowest_author.iloc[i]['id'], 0)
            perc_sum += perc[0]
            pp += perc[1]

    prop = (perc_sum) / np.float32(lenn)
    print("\nCross-author self-citation percentage for author \"" + author + "\": " + str(
        np.round(100.0 * prop, decimals=2)) + "%")
    return prop

# Given an author
def self_cite(author_list, id, jumps, parent_):#author, id):
    if jumps <= 1:
        global df
        tlist = list()
        for author in author_list:#plist:
            #print(author)
            occurances_of = df.loc[df['name'] == author]
            #print("1>")
            #print(occurances_of)
            #if jumps == 0:
            occurances_of = occurances_of.loc[occurances_of['parent'] == id]
            #else:
            #    occurances_of = occurances_of.loc[occurances_of['parent'] == parent_]
            #    occurances_of = occurances_of.loc[occurances_of['id'] == id]
            #print(occurances_of)
            occurances_of = occurances_of.loc[occurances_of['layer'] > 0]
            #print("2>")
            #print(occurances_of)
            #print(author + " | " + str(id))
            #print(df.loc[df['id'] == id])
            tlist.append(occurances_of)

        occs = pd.DataFrame(data=tlist[0])

        for j in range(len(tlist) - 1):
            occs = occs.append(tlist[j + 1])
        # Count unique citations
        ddf = df.loc[df['parent'] == id]
        ddf = ddf.loc[ddf['layer'] > 0]
        gb = ddf.groupby(['id']).count().reset_index()
        authors_names = pd.DataFrame(data=occs.loc[:]['name'], columns=['name']).groupby(['name']).count().reset_index()
        #grouped = pregrouped.groupby('id').count().reset_index()
        #print(pregrouped.loc[''][''])
        author_new_list = list(authors_names.loc[:]['name'])
        ids_to_go_through = pd.DataFrame(data=occs.loc[:]['id'], columns=['id']).groupby(['id']).count().reset_index()
        #print(ids_to_go_through)
        countarray = [0.0, 0.0]
        for cw in range(len(ids_to_go_through)):
            #if jumps < 1:
            #    for author in author_new_list:
            #        list_collaborators = list()
            #        ddd = df.loc[df['name'] == author]
            #        ddd = ddd.loc[ddd['layer'] > 0]
            #        ddd = ddd.loc[ddd['name'] != author]['name']
            #        for a2 in range(len(ddd)):
            #            list_collaborators.append(a2)
#
            #        print(list_collaborators)
#
            new_id = ids_to_go_through.iloc[cw]['id']
            ocarray = self_cite(author_list, new_id, jumps + 1, parent_)
            countarray[0] += ocarray[0]
            countarray[1] += ocarray[1]
            #print(new_id)

        oca = np.float32(len(occs))
        lgb = np.float32(len(gb))

        return [oca + countarray[0], lgb + countarray[1]]
    else:
        return [0.0, 0.0]

def author_find(id_of, jumps):
    if jumps <= 1:
        global df
        pubs_authors = df.loc[df['id'] == id_of][['name', 'id']]

        print("\nInspecting cross-author self-citation percentages...")
        sum = 0.0
        total = 0.0
        lists = list()
        #list_to_look_for = list()
        #authorz = pubs_authors.groupby('name').count.reset_index()
        for zz in range(len(pubs_authors)):
            lists.append(pubs_authors.iloc[zz]['name'])
        #for a in range(len(authorz)):
        #    ddff = df.loc[authorz.iloc[a]['name'] == df['name']]
        #    ggb = ddff.groupby('id').count.reset_index()
        #    for pub in range(len(ggb)):
        #        idtoadd = ggb.iloc[pub]['id']
        #        list_to_look_for.append(idtoadd)
        #utlf = np.unique(list_to_look_for)
        #data_size = 0.0
        #data_found = 0.0
        #for a in range(len(authorz)):
        #    for uu in utlf:
        #        dataa = author_find(uu, a)
        #        data_size += dataa[1]
        #        data_found += dataa[0]

        data = self_cite(lists, id_of, jumps, id_of)
        return [data[0] / data[1], np.float32(len(pubs_authors))]
    else:
        return 0.0
    #print("Perc = " + str(sum / total) + "%")

while (True):
    author_first = raw_input("Enter the first name of an author (ex. \"dorothea\"): ")#"besch, dorothea"#"roehl, kimberly a"#"besch, dorothea"#"schumann, barbara"
    author_last = raw_input("Enter the last name of the author (ex. \"besch\"): ")
    if author == "quit":
        break

    author = author_last + ", " + author_first
    prop2 = find_self_citation_percentage_from_author(df, author)