import sqlite3
import pandas as pd

vertex_list = pd.read_csv("/Users/mkorovkin/Desktop/citations_2015_data_ms450469.csv")

connection = sqlite3.connect('/Users/mkorovkin/Desktop/test5.db')
cursor = connection.cursor()

table_name = 'overall_table22'
cursor.execute('CREATE TABLE {name} (id integer PRIMARY KEY,\ndate text NOT NULl,\nlayer integer,\nparent integer,\nname text NOT NULL)'.format(name=table_name))

# for x in range(0, len(vertex_list))[:10]:
#     #author_name = vertex_list.iloc[x]['name']
#     #comma_index = author_name.find(',')
#     new_name = ''.join([i if ord(i) < 128 else ' ' for i in vertex_list.iloc[x]['name']])
#     new_name = new_name.replace('\'', '')
#     cursor.execute('INSERT OR IGNORE INTO {name} VALUES ({id},\n\'{date}\',\n{layer},\n{parent},\n\'{name_a}\')'.format(name=table_name,
#                                                                                                       id=vertex_list.iloc[x]['id'],
#                                                                                                       date=vertex_list.iloc[x]['date'],
#                                                                                                       layer=vertex_list.iloc[x]['layer'],
#                                                                                                       parent=vertex_list.iloc[x]['parent'],
#                                                                                                       name_a=new_name))

list_table = list()
for row in range(0, len(vertex_list))[:200]:
    new_name = ''.join([i if ord(i) < 128 else ' ' for i in vertex_list.iloc[row]['name']])
    new_name = new_name.replace('\'', '')

    list_table.append((vertex_list.iloc[row]['id'], vertex_list.iloc[row]['date'],
                 vertex_list.iloc[row]['layer'], vertex_list.iloc[row]['parent'], new_name))

    #cursor.execute('INSERT OR IGNORE INTO {name} VALUES (?, ?, ?, ?, ?)'.format(name=table_name), (vertex_list.iloc[row]['id'], vertex_list.iloc[row]['date'],
    #             vertex_list.iloc[row]['layer'], vertex_list.iloc[row]['parent'], new_name))

print(list_table)
print(len(list_table))

cursor.executemany('INSERT OR IGNORE INTO {name} VALUES (?, ?, ?, ?, ?)'.format(name=table_name), list_table)

print(pd.read_sql_query("SELECT * FROM {name}".format(name=table_name), connection))

connection.commit()
connection.close()