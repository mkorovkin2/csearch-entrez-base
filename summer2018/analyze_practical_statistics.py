import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
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
            for parent in parent_names['name']:
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
def analyze_dataframe_statistics(condition='backwards'):
    global df
    global unique_parent_ids

    # Sum of all self-citation rates found (used for the calculation of mean self-citation rate)
    total_occur = 0.0
    # Total author names inspected (used for the calculation of mean self-citation rate)
    total_runs = 0.0

    # List that holds the standard deviation of the data
    std_dev = list()
    list_total = list()

    # Loop through all papers and extract names for self-citation analysis
    for parent_paper in unique_parent_ids:
        # Extract parent names
        parent_authors = df[df['id'] == parent_paper][['name']]

        # Extract child papers
        child_papers = df[df['parent'] == parent_paper][['id', 'name', 'layer', 'fb']]

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
        result_occur, result_total = cross_reference_parent_child(parent_authors, child_papers)

        # Make sure that result_total is greater than 0 to avoid dividing by zero
        # If result_total = 0 there is no point in performing the following operation anyway
        if result_total > 0:
            div = np.float32(result_occur) / np.float32(result_total)

            # Increment total_occur by the current rate of self-citation
            total_occur += div

            # Append the self-citation rate to the standard deviation list for easy calculation of standard deviation
            std_dev.append(div)
            list_total.append(result_total)

            # Increment the total_runs by 1
            total_runs += 1.0

    # Return the mean rate of self-citation as well as the standard deviation of the self-citation rates
    return total_occur / np.float32(total_runs), pd.Series(std_dev).std(), std_dev, list_total

# # # # # # # # # # # # # # # # # # # # script initiation code # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

# Load the data frame
df = pd.read_csv("/Users/mkorovkin/Desktop/basicdata/2008.csv")

# Extract all parent paper ids into unique_parent_ids
unique_parent_ids = pd.DataFrame({'count': df.groupby(['id', 'parent']).size()}).reset_index()['parent'].unique()

# Calculate the mean self-citation rate and standard deviation with only backwards-in-time citations
sc_mean_backwards, sc_std_dev_backwards, sc_list_backwards, sc_list_total_backwards = analyze_dataframe_statistics(condition='backwards')

# Calculate the mean self-citation rate and standard deviation with only forwards-in-time citations
sc_mean_forwards, sc_std_dev_forwards, sc_list_forwards, sc_list_total_forwards = analyze_dataframe_statistics(condition='forwards')

# Output global statistics from the backwards-in-time citation rate calculations
print("Backwards self-citation rates = {\n\tmean: " +
      str(np.round(sc_mean_backwards * 100.0, decimals=2)) + "%,\n\tstd: " +
      str(np.round(sc_std_dev_backwards * 100.0, decimals=2)) + "%\n}")
print

# Output global statistics from the forwards-in-time citation rate calculations
print("Forwards self-citation rates = {\n\tmean: " +
      str(np.round(sc_mean_forwards * 100.0, decimals=2)) + "%,\n\tstd: " +
      str(np.round(sc_std_dev_forwards * 100.0, decimals=2)) + "%\n}")

print(sc_list_backwards)
print(sc_list_total_backwards)