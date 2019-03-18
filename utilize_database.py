import sqlite3
import pandas as pd

class Vertex():
    loc = []
    id = 0
    name = ''
    date = ''
    links = []
    vid = 0

    def link(self, vertex):
        self.links.append(vertex)

    def create(self, x, y, z):
        self.loc = [x, y, z]

def assemble_verteces(overall_df):
    id_count = 0

    vertex_list = list()

    for c in range(len(overall_df.iloc[:]['id'])):
        v = Vertex()
        v.id = overall_df.iloc[c]['id']
        v.name = overall_df.iloc[c]['name']
        v.date = overall_df.iloc[c]['date']
        v.vid = id_count
        #v.links = filter_id(overall_df, id)

        v.create(v.id, overall_df.iloc[c]['parent'], overall_df.iloc[c]['layer'])

        # trim v.name
        if not (v.name.find(',') + 3 >= len(v.name)):
            v.name = v.name[:v.name.find(',') + 3]

        vertex_list.append(v)

        id_count += 1

    return vertex_list

def visualize_parent(vertex, start_layer):
    fin_string = ''
    for i in range(start_layer * 3):
        fin_string += ' '
    fin_string += str(vertex.id)
    print(fin_string)
    for link in vertex.links:
        visualize_parent(link, start_layer + 1)

def collaborative_filter(vertex1, vertex2):
    inter_list = list()
    for link1 in vertex1.links:
        for link2 in vertex2.links:
            if link1.id == link2.id:
                inter_list.append(link1.id)
    return inter_list

def filter_parent_list(parent_list):
    count = 0
    intersection_list = list()
    for parent1 in parent_list:
        for parent2 in parent_list[(count + 1):]:
            array1 = collaborative_filter(parent1, parent2)
            intersection_list.append([parent1, parent2, len(array1)])
        count += 1
    return intersection_list

connection = sqlite3.connect('/Users/mkorovkin/Desktop/test5.db')
cursor = connection.cursor()

table_name = 'overall_table21'
sql_df = pd.read_sql_query("SELECT * FROM {name}".format(name=table_name), connection)
vertex_list = assemble_verteces(sql_df)

parent_vertex_list = list()
parent_id_list = list()

for v in vertex_list:
    parent = v.loc[1]
    if not (parent in parent_id_list):
        parent_id_list.append(parent)

        p = Vertex()
        p.id = parent
        p.create(parent, 0, 0)
        p.links = list()

        p.links.append(v)

        parent_vertex_list.append(p)
    elif parent in parent_id_list:
        count = 0
        for p in parent_vertex_list:
            if parent == p.loc[0]:
                item = parent_vertex_list[count]
                item.links.append(v)
                parent_vertex_list[count] = item
                break
            else:
                count += 1

#print(parent_id_list)

#for parent in parent_vertex_list:
#    print(len(parent.links))

overall_vertex = Vertex()
overall_vertex.id = 0
overall_vertex.create(0, 0, -1)

overall_vertex.links = parent_vertex_list

#print(overall_vertex.links)

#visualize_parent(overall_vertex, 0)

fplist = filter_parent_list(parent_vertex_list)

#for inter in fplist:
#    print(inter[2])

parents_sql = pd.read_sql_query('select * from {name} where id like parent'.format(name=table_name), connection)
print(parents_sql)

#example
pubs_of_one = pd.read_sql_query('select * from {name} where parent like {value}'.format(name=table_name, value=parents_sql.iloc[0]['id']), connection)
print(pubs_of_one)

connection.close()