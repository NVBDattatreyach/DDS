# from QueryParser import parse_query
from typing import final
from matplotlib.pyplot import table
from QueryP import QueryParser
from QueryD import QueryDecomposer
from ExecutionPlanner import ExecutionPlanner
from Utility import *
from TableHandler import *
import localization as Loc
import sys
import sdd
# import build_join_graph as bj
import modifytree as mt
import read_profiles
import copy


query_to_alias={}


# query = sys.argv[1]
# print('{}'.format(query))
# clause_dict, condition_concat = parse_query("{}".format(query))
# clause_dict, condition_concat = parse_query("SELECT * FROM EMPLOYEE GROUP BY EMPLOYEE.Dept_Name")


query_parser = QueryParser("""select * from EMPLOYEE,WORKS_ON,PROJECT where EMPLOYEE.Emp_Id=WORKS_ON.Emp_Id and WORKS_ON.Project_Id=PROJECT.Project_Id""")

query_parser.parse_query()
clause_dict, condition_concat = query_parser.clause_dict, query_parser.condition_concat

attribute_table_map = get_attribute_to_table_mapping(clause_dict['select'], clause_dict['from'])
# print('MAIN:', condition_concat)
query_decomposer = QueryDecomposer(clause_dict, condition_concat, attribute_table_map)
root = query_decomposer.decompose_query()
# root = decompose_query(clause_dict, condition_concat, attribute_table_map)

print('########### Normal Tree ###############')
print_tree(root)
print('########################################')
optimized_tree = get_optimized_tree(root, clause_dict['from'], attribute_table_map, {})
print('############# Optimized Tree ###############')
print_tree(optimized_tree)
print('#############################################')

Loc.localize(optimized_tree)
print("After localization")
print_tree(optimized_tree)

print("queries")
local_queries,view_to_frag,graph=mt.update_tree(optimized_tree)
print_tree(optimized_tree)
for k,v in graph.items():
    print(k,"->",v)


print(local_queries)
print_tree(optimized_tree)
sdd_input=mt.create_sdd_input(optimized_tree,view_to_frag)
# print(sdd_input)
sdd_input=mt.update_sdd_input(sdd_input)
#print(view_to_frag)

profiles=read_profiles.get_profile()
ping_cost=read_profiles.get_cost()
#print(sdd_input)
#print(profiles)
#print(cost)


for k,v in local_queries.items():
    profiles[k]=copy.deepcopy(profiles[v[1]])
    view_cols=v[3]
    frag_cols=profiles[v[1]]['Cols']
    if(view_cols!="*"):
        view_cols=v[3].split(",")
        for col in frag_cols:
            if col not in view_cols:
                profiles[k].pop(col)
                profiles[k]['Cols'].remove(col)
all_selectivties=[]
for join in sdd_input:
    print(join)
    selectivity_per_join=[]
    for semi_join in join:
        selectivity=min(1,profiles[semi_join[3]][semi_join[5]]['val']/profiles[semi_join[3]][semi_join[5]]['dom'])
        selectivity_per_join.append(selectivity)
    all_selectivties.append(selectivity_per_join)

# for row in all_selectivties:
#     print(row)

all_orders=[]
for join,selectivity in zip(sdd_input,all_selectivties[:]):
    
    order=[]
    final_reductions={}
    semi_join_graph={}
    print("sdd input")
    for row in join:
        print(row)
        final_reductions[row[1]]=[row[1],row[0]]
        final_reductions[row[3]]=[row[3],row[2]]
        
    sdd.sdd1(join,ping_cost,profiles,selectivity,order,final_reductions,semi_join_graph)
    
    print("sdd output:")
    for x in order:
        print(x)
    print("final reductions")
    for k,v in final_reductions.items():
        print(k,v)
    tables_at_sites={}
    for k,v in final_reductions.items():
        if(v[1] in tables_at_sites):
            tables_at_sites[v[1]].append(v[0])
        else:
            tables_at_sites[v[1]]=[v[0]]
    print(tables_at_sites)
    cost_at_sites={}
    for site1 in tables_at_sites.keys():
        cost=0
        for site2,tables in tables_at_sites.items():
            if(site1!=site2):
                for table in tables:
                    size=0
                    for col in profiles[table]['Cols']:
                        size+=profiles[table][col]['size']
                    cost+=size*profiles[table]['card']

        cost_at_sites[site1]=cost
    print(cost_at_sites)
    final_site=min(cost_at_sites.keys(),key=(lambda k:cost_at_sites[k]))
    print(final_site)
    
    print("----------------")

"""
sdd input


"""

"""
execution_planner = ExecutionPlanner()


plan = execution_planner.prepare_execution_plan(optimized_tree)
CP5 => [("create view t1 as select * from EMP1",t1),("create view t2 as SELECT * from EMP_DETAILS1"),("t1 join t2"),()]
    ["create view t1 as select * from EMP1, EMP_DETAILS where EMP1.Emp_Id=EMP_DETAILS.Emp_Id"]
"""

