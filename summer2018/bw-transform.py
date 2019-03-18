import pandas as pd

def modify_string(s, c):
    global df

    for i in range(len(s) - 1):
        substring = s[(i + 1):] + c + s[:(i + 1)]
        temp_df = pd.DataFrame(data=[sub for sub in substring]).transpose()
        temp_df.columns=[0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
        df = df.append(temp_df)

string = "TATCTTGCAT"
spec_char = "@"
string_to_find = "AT"

df = pd.DataFrame(columns=[0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10])
modify_string(string, spec_char)
df = df.sort_values(0)

gg = df.loc[df[0] == string_to_find[1]]
print(gg.loc[gg[10] == string_to_find[0]])