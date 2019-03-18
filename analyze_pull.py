from mpl_toolkits import mplot3d

import pandas as pd
import numpy as np

import matplotlib.pyplot as plt

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

class Xet():
    loc = []
    layers = []
    name = ''
    verteces = []
    children = []
    flag = False

    def append(self, vertex):
        self.children.append(vertex)

    def id_tree(self):
        ch = self.children
        ids = list()

        for child in ch:
            ids.append(child.id)

        return ids

    def vertex_tree(self):
        ch = self.verteces
        names = list()

        for child in ch:
            names.append(child.id)

        return names

    def intersect(self, xet):
        t1 = self.id_tree()
        t2 = xet.id_tree()

        inter_list = list()

        for t in t1:
            if (t in t2):
                inter_list.append(t)

        return inter_list

    def intersect_vn(self, xet):
        t1 = self.vertex_tree()
        t2 = xet.vertex_tree()

        inter_list = list()

        for t in t1:
            if (t in t2):
                inter_list.append(t)

        return inter_list

# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

def assemble_verteces(csv_directory):
    overall_df = pd.read_csv(filepath_or_buffer=csv_directory)
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

def filter_id(df, id):
    id_count = 0
    ids_list = list()
    found = False

    for c in range(len(df.iloc[:]['id'])):
        if df.iloc[c]['id'] == id:
            ids_list.append(id_count)
            found = True
        elif found:
            break
        id_count += 1

    return ids_list

def get_unique_citations(vector_list):
    unique_id_list = list()

    for v in vector_list:
        if not (len(unique_id_list) == 0) and not (unique_id_list[-1] == v.id) and not (v.id in unique_id_list):
            unique_id_list.append(v.id)

    return unique_id_list

def get_parent_list(vertex_list):
    all_parents = list()

    for v in vertex_list:
        p = v.loc[1]
        if not p in all_parents:
            all_parents.append(p)

    return all_parents

def transform_verteces_into_xet(vertex_list):
    xet_list = list()
    xet_names = list()

    for v in vertex_list:
        if not v.name in xet_names:
            x = Xet()

            x.name = v.name
            x.layers = [v.loc[1]]
            x.loc = [v.id, v.loc[1], v.loc[2]]

            x.verteces.append(v)

            xet_names.append(v.name)
            xet_list.append(x)
        else:
            index = xet_names.index(v.name)

            xet = xet_list[index]
            xet.verteces.append(v)
            xet.layers.append(v.loc[1])
            xet.flag = True

            xet_list[index] = xet

    return xet_list

def hloc(xyz, value, vertex_list):
    count = 0

    for v in vertex_list:
        if v.loc[xyz] == value:
            return count
        else:
            count += 1

    return -1

def dloc_name(name, data_list):
    count = 0

    for d in data_list:
        if d.name == name:
            return count
        else:
            count += 1

    return -1

def hfilter_flag(xet_list, find_flag=True):
    xet_list_flag = list()

    for x in xet_list:
        if x.flag == find_flag:
            xet_list_flag.append(x)

    return xet_list_flag

def hfilter(xyz, value, xet_list):
    xet_list_flag = list()

    for x in xet_list:
        if x.loc[xyz] == value:
            xet_list_flag.append(x)

    return xet_list_flag

def print_xet_list(xet_list):
    count = 0

    for x in xet_list:
        print("{0:7} name: {1:20} id: {2:10} num_children: {3:5} num_verteces: {4:8}".format(count, x.name, x.loc[0], len(x.children), len(x.verteces)) )
        count += 1

def print_xet(xet):
    print(xet.name)
    for v in xet.verteces:
        print_vertex(v)

def print_vertex(v):
    print(v.name)
    for v2 in v.links:
        print_vertex(v2)

def find_repeats(q_list):
    count_list = list()
    ret_list = list()

    for q in q_list:
        if q in count_list:
            ret_list.append(q)
        else:
            count_list.append(q)

    return ret_list

# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

vertex_list = assemble_verteces("/Users/mkorovkin/Desktop/citations_2015_data_ms450469.csv")

unique_citations = get_unique_citations(vertex_list)
parent_citation = get_parent_list(vertex_list)

xet_list = transform_verteces_into_xet(vertex_list)

print("vertex list: " + str(len(vertex_list)))
print("xet list: " + str(len(xet_list)))

xxxx = dloc_name('lucae, s', vertex_list)
print(xxxx)
print("lucae, susanne: " + str(dloc_name('lucae, s', vertex_list[(xxxx + 1):])))

for x in xet_list:
    count = 0

    if x.name == 'lucae, s':
        print("location {0:15} name: {1:20} id: {2:10} num_children: {3:15} vertex_length {4:8}".format(count, x.name, x.loc[0], len(x.children), len(x.verteces)))
        break
    count += 1

for parent_id in parent_citation:
    L = hfilter(1, parent_id, xet_list)
    print(len(L))
    print(L)
    Q = list()
    for x in L:
        Q.append(x.name)
    print(len(find_repeats(Q)))


flagged_list = hfilter_flag(xet_list)
print_xet_list(flagged_list)
#print(len(flagged_list[0].verteces))
#print(len(flagged_list[1].verteces))
#print(len(flagged_list[0].intersect_vn(flagged_list[1])))

# Cross reference Xet authors with one another and locate them in the vertex list

# lucae, susanne appears like 3 or 4 times in the data but does not show up as a repeat