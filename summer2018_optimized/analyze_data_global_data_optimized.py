import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from scipy.stats import norm
from scipy.stats import normaltest
from scipy.stats import gamma
from scipy.stats import beta
from scipy.stats import expon

# Find the number of duplicate occurrences in author names of a parent publication in their references
def cross_reference_parent_child(parent_names, child_publications):
    duplicate_occurrences = 0

    # Child publications grouped by 'id'
    child_publications_group = child_publications.groupby(['id'])

    for child_table in child_publications_group:
        # Reset loop-breaker each time a loop is broken
        bk = False
        for child_name in child_table[1]['name']:
            for parent in parent_names:
                # Compare child and parent names
                if parent == child_name:
                    # Increment duplicate_occurrences
                    duplicate_occurrences += 1
                    bk = True
                    break
            if bk:
                # Make sure that both loops break
                break

    # Return both elements
    return duplicate_occurrences, len(child_publications_group)

#
def analyze_dataframe_statistics(df, condition='backwards'):
    unique_parent_ids_before = df.loc[df['layer'] == 0].groupby(['parent'])[['id']]
    unique_parent_ids_before = unique_parent_ids_before.count().reset_index().loc[:, 'parent']
    #print(unique_parent_ids_before)
    unique_parent_ids = list()
    for item in unique_parent_ids_before:
        #print(item)
        unique_parent_ids.append(np.int32(item))

    # Sum of all self-citation rates found (used for the calculation of mean self-citation rate)
    total_occur = 0.0
    # Total author names inspected (used for the calculation of mean self-citation rate)
    total_runs = 0.0

    # List that holds the standard deviation of the data
    std_dev = list()
    list_total = list()
    # Loop through all papers and extract names for self-citation analysis
    print(unique_parent_ids)
    for parent_paper in unique_parent_ids:
        # Extract parent names
        parent_authors = df.loc[df['id'] == parent_paper][['name']]
        #print(parent_authors)
        parent_authors = parent_authors.reset_index()

        # Extract child papers
        child_papers = df[df['parent'] == parent_paper][['id', 'name', 'layer', 'fb']]
        if len(child_papers) > 0:
            # Constrain self-citation analysis to either only backwards citations, forward citations, or neither
            if condition == 'backwards':
                child_papers = child_papers[child_papers['layer'] > 0]
                child_papers = child_papers[child_papers['fb'] < 0].reset_index()  # Filter: only backwards citations
            elif condition == 'forwards':
                child_papers = child_papers[child_papers['layer'] > 0]
                child_papers = child_papers[child_papers['fb'] > 0].reset_index()  # Filter: only forwards citations
            else:
                child_papers = child_papers[child_papers['layer'] > 0].reset_index()

            # Perform self-citation analysis for a given combination of an author and their publication's references
            two_parent_authors = []
            if len(parent_authors) > 1:
                two_parent_authors = [
                    parent_authors['name'][0],
                    parent_authors['name'][len(parent_authors) - 1]
                ]
            elif len(parent_authors) == 1:
                two_parent_authors = parent_authors['name'][0]
            else:
                return np.nan, np.nan, np.nan, np.nan
            result_occur, result_total = cross_reference_parent_child(two_parent_authors, child_papers)
            # Make sure that result_total is greater than 0 to avoid dividing by zero
            # If result_total = 0 there is no point in performing the following operation anyway
            if result_total > 0:
                print(10)
                div = np.float32(result_occur) / np.float32(result_total)

                # Increment total_occur by the current rate of self-citation
                total_occur += div

                # Append the self-citation rate to the standard deviation list for easy calculation of standard deviation
                std_dev.append(div)
                list_total.append(result_total)

            # Increment the total_runs by 1
            total_runs += 1.0

    print(total_occur)
    print(total_runs)
    # Return the mean rate of self-citation as well as the standard deviation of the self-citation rates
    return total_occur / np.float32(total_runs), pd.Series(std_dev).std(), std_dev, list_total

def analyze_dataframe_statistics_new(df, condition='backwards'):
    total_occurances = 0.0
    total_tested = 0.0

    if condition == 'backwards':
        df = df.loc[df['fb'] <= 0]
    elif condition == 'forwards':
        df = df.loc[df['fb'] >= 0]

    child_df = df.loc[df['layer'] > 0]
    parent_df = df.loc[df['layer'] == 0]
    parents = parent_df.groupby(['id']).count().reset_index()[['id']]

    rate_list = list()

    for ix in range(len(parents)):
        if ix % 25 == 0:
            print(str(np.round(np.float32(ix) / np.float32(len(parents)) * 100.0, decimals=2)) + "% complete")

        subtotal = 0
        subtotal_runs = 0
        filtered_df = child_df.loc[child_df['parent'] == parents.iloc[ix]['id']][['name']]
        if len(filtered_df) > 0:
            parent_names = parent_df.loc[parent_df['id'] == parents.iloc[ix]['id']][['name']]
            for nx in range(len(parent_names)):
                for ax in range(len(filtered_df)):
                    name = parent_names.iloc[nx]['name']
                    author = filtered_df.iloc[ax]['name']
                    if name == author:
                        total_occurances += 1.0
                        subtotal += 1.0
                    total_tested += 1.0
                    subtotal_runs += 1.0
            rate_list.append(np.float32(subtotal) / np.float32(subtotal_runs))

    return total_occurances, total_tested, rate_list

def publication_score(df):
    df = df.loc[df['fb'] <= 0]

    fdf = df.loc[df['layer'] > 0]
    dgb = fdf.groupby(['id'])

    author_names = df.loc[df['layer'] == 0]
    an_grouped = author_names.groupby(['id'])
    parent_id_list = list()

    for tab in an_grouped:
        parent_id_list.append(tab[1]['id'].unique()[0])

    author_repeat_count = 0
    num_pubs = 0
    pub_list = list()

    for id in parent_id_list:
        child_df = fdf.loc[fdf['parent'] == id]
        if len(child_df) > 0:
            author_names_array = np.array(author_names.loc[author_names['id'] == id]['name'])
            gg = child_df.groupby(['id'])
            for tab2 in gg:
                num_pubs += 1
                name_array = np.array(tab2[1]['name'])
                for name in name_array:
                    bk = False
                    for aaa in author_names_array:
                        if name in aaa:
                            author_repeat_count += 1
                            bk = True
                            break
                    if bk:
                        break
            pub_list.append(np.float32(author_repeat_count) / np.float32(num_pubs))

    prop = np.float32(author_repeat_count) / np.float32(num_pubs)

    return prop, pub_list

# # # # # # # # # # # # # # # # # # # # script initiation code # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

def analyze_dataframe_statistics_withauthors(df):
    backward_df = df.loc[df['fb'] < 0]
    unique_parent_ids_before = df.loc[df['layer'] == 0].groupby(['parent'])[['name']]
    unique_authors_of_interest = {}
    parent_ids = {}
    for item in unique_parent_ids_before:
        parent_ids[list(item[1]['id'])[0]] = 1
        names = list(item[1]['name'])
        unique_authors_of_interest[names[0]] = 1
        unique_authors_of_interest[names[len(names) - 1]] = 1

    ids_of_interest = list(parent_ids.keys())

    total_ids_inspected = 0
    self_citation_total = 0
    sc_list = list()
    for id in ids_of_interest:
        sub_df = backward_df.loc[backward_df['parent'] == id]
        if len(sub_df) > 0:
            sub_df_names = sub_df.groupby(['id'])[['name']]
            sub_insp = 0
            sub_sc = 0
            for sub_name in sub_df_names:
                total_ids_inspected += 1
                sub_insp += 1
                all_names = list(sub_name[1]['name'])
                for name in all_names:
                    if unique_authors_of_interest.__contains__(name):
                        self_citation_total += 1
                        sub_sc += 1
                        break
            sc_list.append(sub_sc / sub_insp)

    return self_citation_total / total_ids_inspected, sc_list

# Load the data frame
df = pd.read_csv("/Users/mkorovkin/Desktop/citations_2010_data_ms925687.csv")#citations_2007_data_ms266414.csv")#2009.csv")

# Extract all parent paper ids into unique_parent_ids
#unique_parent_ids = pd.DataFrame({'count': df.groupby(['id', 'parent']).size()}).reset_index()['parent'].unique()

result, result_list = analyze_dataframe_statistics_withauthors(df)#analyze_dataframe_statistics_new(df)

# print(norm.fit(pub_list))
print("am:", result)
result_list = np.array(result_list)
print("uwm:", result_list.mean())
print("std:", result_list.std())

a_g, _, _ = gamma.fit(result_list)
a, b, _, _ = beta.fit(result_list)
print("alpha", a)
print("beta", b)
_, l = expon.fit(result_list)

x_space_gamma = np.linspace(gamma.ppf(0.01, a_g), gamma.ppf(0.99, a_g), 100)
x_space = np.linspace(0, 1, num=1000)

plt.hist(result_list, bins=20, alpha=0.8, label='observed self-citation frequency')
plt.plot(x_space, beta.pdf(x_space, a, b), 'r-', lw=2, alpha=0.6, label='fit beta distribution PDF')
plt.xlim(0, 1.0)
#plt.title("Publication Self-Citation Rate Frequency")
#plt.xlabel("Self-citation proportion")
#plt.ylabel("Self-citation rate sample frequency")
plt.ylim(0, 30)
plt.legend()
plt.show()

def actual_cdf(bound, result_list):
    cdf_count = 0
    for i in result_list:
        if i < bound:
            cdf_count += 1

    return cdf_count / len(result_list)

for i in [0.2, 0.4, 0.5, 0.6, 0.8, 1.0]:
    print("b..fit", beta.cdf(i, a, b))
    print("acutal", actual_cdf(i, result_list))

#sc_mean_backwards, sc_std_dev_backwards, sub_total_list = analyze_dataframe_statistics_new(df)
#print(str(np.int32(sc_mean_backwards)) + "/" + str(np.int32(sc_std_dev_backwards)))
#print(str(np.round(sc_mean_backwards / sc_std_dev_backwards * 100.0, decimals=2)) + "% self-citation [cumulative mean]")

#print(plt.hist(sub_total_list, bins=50))
