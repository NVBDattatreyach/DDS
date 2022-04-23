# from QueryParser import parse_query
from collections import defaultdict
from ipaddress import ip_address
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
import System_Catalog as SC
# import build_join_graph as bj
import modifytree as mt
import read_profiles
import copy
from prettytable import PrettyTable
import QueryExecution

query_to_alias={}
# select * from EMPLOYEE,WORKS_ON,PROJECT where EMPLOYEE.Emp_Id=WORKS_ON.Emp_Id and WORKS_ON.Project_Id=PROJECT.Project_Id and EMPLOYEE.Dept_Name='SALES'
# select * from EMPLOYEE,WORKS_ON where EMPLOYEE.Emp_Id=WORKS_ON.Emp_Id and EMPLOYEE.Dept_Name='SALES'
# select * from EMPLOYEE where EMPLOYEE.Dept_Name='SALES' and EMPLOYEE.Loc_Id='MUM'
# select * from PROJECT

# query = sys.argv[1]
# print('{}'.format(query))
# clause_dict, condition_concat = parse_query("{}".format(query))
# clause_dict, condition_concat = parse_query("SELECT * FROM EMPLOYEE GROUP BY EMPLOYEE.Dept_Name")
while(True):
    inp=str(input("Enter Your Query\nCTRL+C to EXIT\n"))
    query=""""""
    query=query+inp
    query_parser = QueryParser(query)
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
    view_count=[0]
    local_queries,view_to_frag,graph=mt.update_tree(optimized_tree,view_count)
    print_tree(optimized_tree)
    new_graph=defaultdict(dict)
    for k,v in graph.items():
        for x in v:
            new_graph[k][x[0]]=x[1]

    sdd_input=mt.create_sdd_input(optimized_tree,view_to_frag)
    sdd_input=mt.update_sdd_input(sdd_input)
    profiles=read_profiles.get_profile()
    ping_cost=read_profiles.get_cost()
    
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
    print("sdd input",sdd_input)
    for join in sdd_input:
        selectivity_per_join=[]
        for semi_join in join:
            selectivity=min(1,profiles[semi_join[3]][semi_join[5]]['val']/profiles[semi_join[3]][semi_join[5]]['dom'])
            selectivity_per_join.append(selectivity)
        all_selectivties.append(selectivity_per_join)

    final_output=[]
    join_present=False
    
    for join,selectivity in zip(sdd_input,all_selectivties[:]):
        
        order=[]
        join_present=True
        final_reductions={}
        semi_join_graph={}
        for row in join:
            final_reductions[row[1]]=[row[1],row[0]]
            final_reductions[row[3]]=[row[3],row[2]]   
        sdd.sdd1(join,ping_cost,profiles,selectivity,order,final_reductions,semi_join_graph)
        tables_at_sites={}
        for k,v in final_reductions.items():
            if(v[1] in tables_at_sites):
                tables_at_sites[v[1]].append(v[0])
            else:
                tables_at_sites[v[1]]=[v[0]]
        #print(tables_at_sites)
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
        #print(cost_at_sites)
        final_site=min(cost_at_sites.keys(),key=(lambda k:cost_at_sites[k]))
        #print(final_site)
        
        final_output.append((order,final_site,tables_at_sites,profiles))

    site_wise_queries=defaultdict(list)
    site_wise_views=defaultdict(list)
    site_wise_drop_queries=defaultdict(list)
    for k,v in local_queries.items():
        site=profiles[v[1]]['site']
        site_wise_queries[site].append(v[2])
        site_wise_views[site].append(k)
        site_wise_drop_queries[site].append("Drop table k")
    for site,queries in site_wise_queries.items():
        for query in queries:
            print(site,"=>",query)

    print(view_count)
    for x in final_output:
        print(x[0],x[1])

    # print(tables_at_sites)
    # execution
    QueryExecution.execute_initial_queries(site_wise_queries)
    print("-----------executing join-----------------")
    results=QueryExecution.execute_joins(final_output,new_graph)
    print("----------union---------------")
    union_node=mt.find_union(optimized_tree)
    if(union_node!=None):
        site_wise_tables=defaultdict(list)
        if(join_present==True):
            for t,out in zip(results,final_output):
                site_wise_tables[out[1]].append(t)
        else:
            site_wise_tables=site_wise_views.copy()
            
        site_wise_results=QueryExecution.getrowspersite(site_wise_tables)
        

        site_wise_cost=defaultdict(lambda:0)
        for site1 in site_wise_results.keys():
            for site2 in site_wise_results.keys():
                if(site1!=site2):
                    site_wise_cost[site1]+=ping_cost[site2][site1]*site_wise_results[site2]
        
        union_site=min(site_wise_cost.keys(),key=(lambda k:site_wise_cost[k]))
        print("best union site",union_site)
        QueryExecution.Union(site_wise_tables,union_site,optimized_tree)

            
    
    else:
        if(len(results)!=0):
            project_columns=optimized_tree.data[8:].split(",")
            actual_columns=set()
            for col in project_columns:
                p=col.find(".")
                if(p!=-1):
                    actual_columns.add(col[p+1:])
                else:
                    actual_columns.add(col)
            col_str=",".join(actual_columns)
            query='select {} from {}'.format(col_str,results[0])
            print("query",query)
            final_site=final_output[0][1]
            ip_address_mapping={'CP5':"10.3.5.211","CP6":"10.3.5.208","CP7":"10.3.5.204","CP8":"10.3.5.205"}
            conn=SC.connect(ip_address_mapping[final_site])
            cursor=conn.cursor(pymysql.cursors.DictCursor)
            cursor.execute(query)
            res=cursor.fetchall()
            if(len(res)>0):
                columns=list(res[0].keys())
                if len(actual_columns)==1 and '*' in actual_columns:
                    actual_columns=columns
                mytable=PrettyTable(actual_columns)
                for row in res:
                    temp=[row[col] for col in actual_columns]
                    mytable.add_row(temp)
                print("---RESULT-----")
                print(mytable)
                conn.close()
            else:
                print("----RESULT-----")
                print("0 rows fetched")
        else:
            
            project_columns=optimized_tree.data[8:].split(",")
            actual_columns=set()
            ip_address_mapping={'CP5':"10.3.5.211","CP6":"10.3.5.208","CP7":"10.3.5.204","CP8":"10.3.5.205"}
            for col in project_columns:
                p=col.find(".")
                if(p!=-1):
                    actual_columns.add(col[p+1:])
                else:
                    actual_columns.add(col)
            col_str=",".join(actual_columns)
            query='select {} from  v0'.format(col_str)
            final_site=""
            for k in site_wise_queries.keys():
                final_site=k
            conn=SC.connect(ip_address_mapping[final_site])
            cursor=conn.cursor(pymysql.cursors.DictCursor)
            cursor.execute(query)
            results=cursor.fetchall()
            print("----------RESULT-----------")
            if(len(results)>0):
                columns=list(results[0].keys())
                mytable=PrettyTable(columns)
                for row in results:
                    temp=[row[col] for col in columns]
                    mytable.add_row(temp)
                print(mytable)
            else:
                print("0 rows fetched")
            conn.close()
    QueryExecution.drop_tables()


    """
    sdd input


    """

    """
    execution_planner = ExecutionPlanner()


    plan = execution_planner.prepare_execution_plan(optimized_tree)
    CP5 => [("create view t1 as select * from EMP1",t1),("create view t2 as SELECT * from EMP_DETAILS1"),("t1 join t2"),()]
        ["create view t1 as select * from EMP1, EMP_DETAILS where EMP1.Emp_Id=EMP_DETAILS.Emp_Id"]
    """


