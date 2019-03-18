import pandas as pd
import numpy as np

def cross_reference_parent_child(values1, values2):
    duplicate_occurances = 0

    for p in values1:
        parent_name = p[0]
        for c in values2:
            child_name = c[0]
            if parent_name == child_name:
                duplicate_occurances += 1

    return duplicate_occurances, len(values2)

def cross_reference_parent_child_practical(values1, values2):
    duplicate_occurances = 0

    for c in values2:
        child_name = c[0]
        for p in values1:
            parent_name = p[0]
            if parent_name == child_name:
                duplicate_occurances += 1
                break

    return duplicate_occurances, len(values2)

def a_min(a, b):
    if a > b:
        return b
    elif b > a:
        return a
    else:
        return a

def a_max(a, b):
    if a > b:
        return a
    elif b > a:
        return b
    else:
        return a

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

df = pd.read_csv("/Users/mkorovkin/Desktop/citations_2010_data_ms666919.csv")

parent_rows = df.loc[df['parent'] == df['id']]

parent_papers = df.groupby(by=['parent', 'id'])[['name']]
parent_paper_ids = df.groupby(by=['id'])[['id']]

unique_parent_ids = pd.DataFrame({'count': df.groupby(['id', 'parent']).size()}).reset_index()['parent'].unique()
unique_paper_ids = pd.DataFrame({'count': df.groupby(['id', 'parent']).size()}).reset_index()['id'].unique()

total_author_appearances = 0
authors_to_verify = list()

total_mathematical = 0.0
total_practical = 0.0
total_occurances_practical = 0.0

std_dev_mathematical = list()
std_dev_practical = list()

validity_flag_sum = 0

for parent_paper in unique_parent_ids:
    parent_authors = df[df['id'] == parent_paper][['name']]
    child_papers = df[df['parent'] == parent_paper][['id', 'name', 'layer', 'fb']]
    child_papers = child_papers[child_papers['layer'] > 0]
    child_papers = child_papers[child_papers['fb'] < 0].reset_index()

    occurances, total = cross_reference_parent_child(parent_authors, child_papers)

    if total > 0:
        div = np.float32(occurances) / np.float32(total)
        total_mathematical += div
        std_dev_mathematical.append(div)

    occurances_practical, total_practical_returned = cross_reference_parent_child_practical(parent_authors.values, child_papers[['name']].values)

    if total_practical_returned > 0:
        total_occurances_practical += occurances_practical
        total_practical += total_practical_returned
        to_append_practical = np.float32(occurances_practical) / np.float32(total_practical_returned)
        std_dev_practical.append(to_append_practical)

        if to_append_practical > 0.1331:
            validity_flag_sum += 1

mean_mathematical = total_mathematical / np.float32(len(unique_parent_ids))
print("Mathematical average self-citation rate: " + str(np.round(mean_mathematical * 100.0, decimals=2)) + "%")
std_math = pd.Series(std_dev_mathematical).std()
print("Std.dev.math: " + str(np.round(std_math * 100.0, decimals=2)) + "%")
print

mean_practical = total_occurances_practical / total_practical
print("Practical average self-citation rate: " + str(np.round(mean_practical * 100.0, decimals=2)) + "%")
std_prac = pd.Series(std_dev_practical).std()
print("Std.dev.prac: " + str(np.round(std_prac * 100.0, decimals=2)) + "%")
print

z_range = 0.84
print("80% interval for practical validity: [" + str(np.round(a_max(mean_practical - std_prac * z_range, 0.0), decimals=4)) + ", " + str(np.round(a_min(mean_practical + std_prac * z_range, 1.0), decimals=4)) + "]")
print("Self-citation proportion is valid up to " + str(np.round(a_min(mean_practical + std_prac * z_range, 1.0), decimals=4)))
print

print("Total validity flags in analysis based on 80% upper proportion boundary 0.1331: " + str(validity_flag_sum))
print("Total number of parent papers: " + str(len(unique_parent_ids)))