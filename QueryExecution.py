from ipaddress import ip_address
import threading
from typing import final
import requests
import time
import pymysql
from prettytable import PrettyTable
import System_Catalog as SC
ip_address_mapping={"CP5":"10.3.5.211","CP6":"10.3.5.208","CP7":"10.3.5.204","CP8":"10.3.5.205"}

def query(site,queries):
    for query in queries:
        url="http://{}:8000/create_view".format(ip_address_mapping[site])
        r=requests.post(url,json={'query':query})


def execute_initial_queries(queries_at_sites:dict):
    threads=[]
    for k,v in queries_at_sites.items():
        temp_thread=threading.Thread(target=query,args=(k,v))
        threads.append(temp_thread)
        temp_thread.start()
    for t in threads:
        t.join()
def get_table_name(name):
    p=name.find("_")
    if(p!=-1):
        name=name[:p]
    return name




def execute_semi_joins(join,graph,temp_table_name,i,results):
    final_site=join[1]
    tables_at_sites=join[2]
    profiles=join[3]
    select_columns=[]
    join_conditions=[]
    temp_count=0
    columns_dict={}

    for view in tables_at_sites[final_site]:
        p=view.find("_")
        if(p!=-1):
            view=view[:p]
        for col in profiles[view]['Cols']:
            if col not in columns_dict:
                columns_dict[col]=view
    
    tables_at_final_site=tables_at_sites[final_site]
    actual_tables_at_final_site=[]
    final_views=[]

    if(len(tables_at_final_site)>1):
        for j in range(len(tables_at_final_site)):
            table=tables_at_final_site[j]
            p=table.find("_")
            if(p!=-1):
                table=table[:p]
            actual_tables_at_final_site.append(table)

        for k,v in columns_dict.items():
            select_columns.append(v+"."+k)

        for j in range(len(actual_tables_at_final_site)):
            # print(tables_at_final_site[i])
            table1=actual_tables_at_final_site[j]
            for k in range(j+1,len(actual_tables_at_final_site)):
                # print(tables_at_final_site[j])
                table2=actual_tables_at_final_site[k]
                if(table2 in graph[table1]):
                    col1=graph[table1][table2]
                    col2=graph[table2][table1]
                    join_conditions.append("{}.{}={}.{}".format(table1,col1,table2,col2))
        
        final_join_condition=" and ".join(join_conditions)
        final_tables=",".join(actual_tables_at_final_site)
        final_columns=",".join(select_columns)
        temp_table=temp_table_name+str(temp_count)
        temp_count+=1
        temp_tables_per_site={}
        print(i,temp_table)
        
        final_views.append(temp_table)
        for table1 in actual_tables_at_final_site:
            for table2 in graph[table1].keys():
                if(table2 not in actual_tables_at_final_site):
                    graph[temp_table][table2]=graph[table1][table2]
                    graph[table2][temp_table]=graph[table2][table1]

        print(i,graph[temp_table])

        for k in columns_dict.keys():
            columns_dict[k]=temp_table

        query="create Table {} as select {} from {} where {}".format(temp_table,final_columns,final_tables,final_join_condition)
        url="http://{}:8000/create_view".format(ip_address_mapping[final_site])
        requests.post(url,json={'query':query})
        print(i,final_site,"=>",query)
        for semi_join in join[0]:
            if(semi_join[0]!=final_site):
                table1=get_table_name(semi_join[1])
                table2=get_table_name(semi_join[3])
                if(table2 in tables_at_final_site):
                    semi_join[3]=temp_table
        
        
        print(i,join[0])
        
        for semi_join in join[0]:
            if(semi_join[0]!=final_site):
                temp_table=temp_table_name+str(temp_count)
                temp_count+=1
                site1=semi_join[0]
                site2=semi_join[2]
                query1="create Table {} as select {} from {}".format(temp_table,semi_join[5],semi_join[3])
                print(i,site2,"->",query1)
                # create dump file at site2
                url1="http://{}:8000/create_dump".format(ip_address_mapping[site2])
                r1=requests.post(url1,json={'query':query1,'file_name':temp_table})
                path1='/home/user/xmen/temp/{}.sql'.format(temp_table)
                
                #send dump file at site1
                print(i,"shipping {}@{} to {}".format(temp_table,site2,site1))
                url2="http://{}:8000/send_to_peer".format(ip_address_mapping[site2])
                r2=requests.post(url2,json={'local_path':path1,'remote_ip':ip_address_mapping[site1],'remote_user':'user','remote_path':'/home/user/xmen/temp/'})
                remote_path="/home/user/xmen/temp/{}.sql".format(temp_table)
                #load dump file at site1
                url3="http://{}:8000/load_dump".format(ip_address_mapping[site1])
                print(i,"load@",site1,temp_table)
                r3=requests.post(url3,json={'filepath':remote_path,'view_name':semi_join[3]})
                #join dump file at site1
                query2="create Table {} as select {}.* from {},{} where {}.{}={}.{}".format(semi_join[6],semi_join[1],semi_join[1],temp_table,semi_join[1],semi_join[4],temp_table,semi_join[5])
                print(i,site1,"->",query2)
                url4="http://{}:8000/create_view".format(ip_address_mapping[site1])
                r4=requests.post(url4,json={'query':query2})

    else:
        final_views.append(tables_at_final_site[0])
        

    for site in tables_at_sites.keys():
        if(site!=final_site):
            for table in tables_at_sites[site]:
                #create dump at site
                final_views.append(table)
                url1="http://{}:8000/create_direct_dump".format(ip_address_mapping[site])
                r1=requests.post(url1,json={'table_name':table})
                # #send dump to final site
                path1='/home/user/xmen/temp/{}.sql'.format(table)
                print(i,"shipping {}@{} to {}".format(table,site,final_site))
                url2="http://{}:8000/send_to_peer".format(ip_address_mapping[site])
                r2=requests.post(url2,json={'local_path':path1,'remote_ip':ip_address_mapping[final_site],'remote_user':'user','remote_path':'/home/user/xmen/temp/'})
                # if(r2.json()['status']=='OK'):
                remote_path="/home/user/xmen/temp/"+table+'.sql'
                #load dump at final site
                print(i,"loading",table,"@",final_site)
                url3="http://{}:8000/load_dump".format(ip_address_mapping[final_site])
                r3=requests.post(url3,json={'filepath':remote_path})
                

    #---------final join-----------
    print(i,"final_views",final_views)
    
    last_join=[]
    if(len(final_views)>1):
        last_columns=[]
        for j in range(len(final_views)):
            table1=final_views[j]
            for k in range(j+1,len(final_views)):
                final_table2=final_views[k]
                table2=get_table_name(final_table2)
                if(table2 in graph[table1]):
                    join_str="{}.{}={}.{}".format(table1,graph[table1][table2],final_table2,graph[table2][table1])
                    last_join.append(join_str)

        last_join_condition=" and ".join(last_join)
        last_tables=",".join(final_views)
        final_table=temp_table_name+str(temp_count)
        
        for view in final_views[1:]:
            for col in profiles[view]['Cols']:
                if(col not in columns_dict):
                    columns_dict[col]=view


        for k,v in columns_dict.items():
            last_columns.append(v+"."+k)
        
        
        columns_str=",".join(last_columns)
        query="create table {} as select {} from {}".format(final_table,columns_str,last_tables)
        if(len(last_join_condition)>0):
            query=query+" where {}".format(last_join_condition)
        print(i,"final join query",query)
        url="http://{}:8000/create_view".format(ip_address_mapping[final_site])
        print("exectuing final join",end="")
        r=requests.post(url,json={'query':query})
        print(r.json()['status'])
        print(i,"final table name",final_table)
    else:
        final_table=final_views[0]
        print(i,"final table",final_views[0])
    results[i]=final_table

    
def execute_joins(final_output,graph):
    threads=[]
    results=[None]*len(final_output)
    for i,join in enumerate(final_output):
        temp_table="temp{}".format(i)
        temp=graph.copy()
        temp_thread=threading.Thread(target=execute_semi_joins,args=(join,temp,temp_table,i,results))
        temp_thread.start()
        threads.append(temp_thread)
    for t in threads:
        t.join()

    return results

def getrowscount(site,tables):
    pass

def build_count_query(tables):
    query_list=[]
    if(len(tables)==1):
        final_query="select count(*) as count from {}".format(tables[0])
        return final_query
    else:
        for table in tables:
            query_list.append("select count(*) as count from {}".format(table))
        query=" UNION ALL ".join(query_list)
        final_query="select sum(count) as count from ("+query+") AS T"
        return final_query


def getrowspersite(site_wise_tables):
    query_at_site={}
    for site in site_wise_tables.keys():
        query_at_site[site]=build_count_query(site_wise_tables[site])
        
    rows_at_site={}
    for k,v in query_at_site.items():
        conn=SC.connect(ip_address_mapping[k])
        cursor=conn.cursor()
        cursor.execute(v)
        result=cursor.fetchall()
        rows_at_site[k]=result[0][0]
        conn.close()
    return rows_at_site
    
def send_other_sites(site,tables,final_site):
    for table in tables:
        url="http://{}:8000/create_direct_dump".format(ip_address_mapping[site])
        requests.post(url,json={'table_name':table})
        url="http://{}:8000/send_to_peer".format(ip_address_mapping[site])
        local_path="/home/user/xmen/temp/{}.sql".format(table)
        requests.post(url,json={'local_path':local_path,'remote_user':'user','remote_path':local_path,'remote_ip':ip_address_mapping[final_site]})
        

        

def Union(site_wise_tables,final_site,optimized_tree):
    threads=[]
    union_temp_count=0
    others_tables=[]
    
    for site in site_wise_tables.keys():
        if(site!=final_site):
            temp_thread=threading.Thread(target=send_other_sites,args=(site,site_wise_tables[site],final_site))
            others_tables.extend(site_wise_tables[site])
            temp_thread.start()
            union_temp_count+=1
            threads.append(temp_thread)

    for t in threads:
        t.join()

    for table in others_tables:
        url="http://{}:8000/load_dump".format(ip_address_mapping[final_site])
        path="/home/user/xmen/temp/{}.sql".format(table)
        requests.post(url,json={'filepath':path})
        site_wise_tables[final_site].append(table)
    
    print(site_wise_tables[final_site])

    project_columns=optimized_tree.data[8:].split(",")
    actual_columns=set()

    for col in project_columns:
        p=col.find(".")
        if(p!=-1):
            actual_columns.add(col[p+1:])
        else:
            actual_columns.add(col)
    col_str=",".join(actual_columns)
    conn=SC.connect(ip_address_mapping[final_site])
    query_list=[]
    for table in site_wise_tables[final_site]:
        query="select {} from {}".format(col_str,table)
        query_list.append(query)
    final_union_query=" UNION ALL ".join(query_list)
    cursor=conn.cursor(pymysql.cursors.DictCursor)
    
    cursor.execute(final_union_query)
    result=cursor.fetchall()
    print("--------RESULT----------")
    if(len(result)>0):
        columns=list(result[0].keys())
        mytable=PrettyTable(columns)
        for row in result:
            temp=[row[col] for col in columns]
            mytable.add_row(temp)
        print(mytable)
    else:
        print("0 Rows Fetched")
    conn.close()


def drop_tables():

    print("cleaning temp tables")
    ip_addresses=["10.3.5.211","10.3.5.208","10.3.5.204","10.3.5.205"]
    for ip in ip_addresses:
        conn=SC.connect(ip)
        cursor=conn.cursor()
        query="SHOW TABLES"
        cursor.execute(query)
        result=cursor.fetchall()
        drop_tables=[]
        for row in result:
            if(row[0][:4]=='temp' or row[0][:1]=='v'):
                drop_tables.append(row[0])
        tables_str=",".join(drop_tables)
        conn.close()
        print(tables_str,ip)
        if(len(tables_str)>0):
            url="http://{}:8000/drop_table".format(ip)
            requests.post(url,json={'tables':tables_str})
            print("cleaned {}".format(ip))

    print("clean up completed")




    
    
