import numpy as np
import os
import summer2018_optimized.analyze_data_individual_data_optimized2 as op
import matplotlib.pyplot as plt
from scipy.stats import gamma

base_path = "/Users/mkorovkin/Desktop/f o l d e r/"
item_list = os.listdir(base_path)

fixed_list = {}

for file in item_list:
    temp_file = file
    if (not file.startswith(".")) and (".csv" in file):
        file = file[10:]
        if "False" in file:
            file = file.replace("False", "")
        if "True" in file:
            file = file.replace("True", "")
        try:
            file = file[:file.index(".csv")]
        except ValueError:
            continue
        file = file.replace("_", " ")
        fixed_list[temp_file] = file

tf = 0
ti = 0
score_list = list()

for path in fixed_list.keys():
    search_path = base_path + path
    name = fixed_list[path]
    score, total_found, total_inspected = op.go(search_path, name)

    tf += total_found
    ti += total_inspected
    score_list.append(score)

t_array = np.array(score_list, dtype=np.float32)

print("Proper cumulative self-citation rate: " + str(np.round(np.float32(tf) / np.float32(ti) * 100.0, decimals=2)) + "%")
print("\nOverall cumulative self-citation rate: " + str(np.round(t_array.mean() * 100.0, decimals=2)) + "%")
print("Overall median self-citation rate: " + str(np.round(np.median(t_array) * 100.0, decimals=2)) + "%")
print("Overall standard deviation of self-citation rate: " + str(np.round(t_array.std() * 100.0, decimals=2)) + "%")

x1, y1, z1 = plt.hist(t_array, bins=22)
print(x1)
print(y1)
print(z1)
plt.show()

print(str(score_list))