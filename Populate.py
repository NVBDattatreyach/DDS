import csv
from ipaddress import ip_address
import sys
import System_Catalog as SC

def execute_query(ip_address:list,queries:str):
    for address in ip_address:
        conn=SC.connect(address)
        cur=conn.cursor()
        SC.create_tables(cur)
        conn.commit()
        q="""
        INSERT INTO SITE_INFO VALUES('CP5','10.3.5.211','user',22,'iiit123');
        INSERT INTO SITE_INFO VALUES('CP6','10.3.5.208','user',22,'iiit123');
        INSERT INTO SITE_INFO VALUES('CP7','10.3.5.204','user',22,'iiit123');
        INSERT INTO SITE_INFO VALUES('CP8','10.3.5.205','user',22,'iiit123');
        """
        SC.exec_query(cur,conn,q)
        conn.commit()
        SC.exec_query(cur,conn,queries)

    
    

def main():
    ip_addr_list=['10.3.5.211','10.3.5.208','10.3.5.204','10.3.5.205']
    table_id=0
    fragment_id=0
    all_queries=[]
    for i in range(1,len(sys.argv)):
        filename=sys.argv[i]
        tablename=filename[:-4]
        #print(tablename)
        application_table_query="INSERT INTO APPLICATION_TABLE VALUES ({},'{}')".format(table_id,tablename)
        all_queries.append(application_table_query)
        #execute_query(ip_addr_list,application_table_query)
        with open(filename,"r") as f:
            
            start_frag_id=fragment_id
            csv_reader=csv.reader(f)
            header=next(csv_reader)
            predicate_set=set()
            all_predicates=[]

            #print(header)
            for row in csv_reader:
                #print(row)
                end_frag_id=fragment_id
                fragment_name=row[0]
                fragment_type=row[1]
                allocated_site=row[2]
                fragment_condition=row[3].upper()
                column_names=row[4]
                hf_columns=False
                fragment_query="INSERT INTO FRAGMENTS VALUES ({},{},'{}',NULL,'{}')".format(fragment_id,table_id,fragment_name,fragment_type)
                all_queries.append(fragment_query)
                #execute_query(ip_addr_list,fragment_query)
                
                mapping_query="INSERT INTO ALLOCATION_MAPPING VALUES ({},'{}')".format(fragment_id,allocated_site)
                all_queries.append(mapping_query)
                #execute_query(ip_addr_list,mapping_query)
                
                columns_set=set()
                
                if(fragment_type=="HF" or fragment_type=="DHF"):
                    predicate_string=fragment_condition.replace(" AND ",",")
                    predicates=predicate_string.split(",")
                    all_predicates.append((predicates,fragment_id))
                    for predicate in predicates:
                        predicate=predicate.strip()
                        predicate_set.add(predicate)
                    
                    if(hf_columns==False):
                        column_names=column_names.split(' ')
                        columns_set=set(column_names)
                        hf_columns=True
                        
                else:
                    column_names=column_names.split(' ')
                    for column in column_names:
                        columns_set.add(column)
                fragment_id+=1
                
            predicate_id=0
            predicate_to_id={}
            for predicate in predicate_set:
                        predicate_to_id[predicate]=predicate_id
                        predicate_query="INSERT INTO PREDICATES VALUES ({},'{}')".format(predicate_id,predicate)
                        #execute_query(ip_addr_list,predicate_query)
                        all_queries.append(predicate_query)

                        predicate_id+=1
            
            for predicates,frag_id in all_predicates:
                for predicate in predicates:
                    predicate=predicate.strip(' ')
                    predicate_id=predicate_to_id[predicate]
                    hf_predicates_query="INSERT INTO HF_PREDICATES VALUES ({},{})".format(predicate_id,frag_id)
                    all_queries.append(hf_predicates_query)

                    #execute_query(ip_addr_list,hf_predicates_query)

            for column in columns_set:
                columns_query="INSERT INTO COLUMNS VALUES({},'{}',NULL,false)".format(table_id,column)
                all_queries.append(columns_query)
                #execute_query(ip_addr_list,columns_query)

            #vf_columns
                    

            
        table_id+=1
        s=";".join(all_queries)
        print(s)
        execute_query(ip_addr_list,s)
                

                
if __name__=="__main__":
    main()