# from ReadCSV import df
import pymysql
from pymysql.constants import CLIENT
from sqlalchemy import create_engine
import pandas as pd
import csv

# ip_address=['10.3.5.211','10.3.5.208','10.3.5.204','10.3.5.205']



class TableFiller:

    def __init__(self, df):
        self.conn=self.connect('10.3.5.211', 'xavier', 'xmen123')
        print('############## CONNECTED TO SERVER ##################')
        print()
        print()
        self.df = df
        self.cur=self.conn.cursor()
        self.fragID_fragName_dict = {}
    
    def connect(self, ip_address, usr, pswd):
        conn=pymysql.connect(
            host=ip_address,
            user=usr,
            password=pswd,
            db='XMEN',
            client_flag=CLIENT.MULTI_STATEMENTS,)
        
        return conn

    def execute_query(self, query):
        data = self.cur.execute(query)
        table_contents = self.cur.fetchall()
        output = [list(x) for x in table_contents]
        
        return output
    
    def execute_remote_query(self, query, cur):
        data = cur.execute(query)
        table_contents = cur.fetchall()
        output = [list(x) for x in table_contents]
        
        return output
    
    def get_sql_table_from_df(self, df, create_engine, table_name):
        engine = create_engine('sqlite://', echo = False)
        df.to_sql(table_name, con = engine)

        return engine
    
    def get_remote_creds(self, frag_id):
        query = "SELECT SITE_INFO.Host_Name, Ip_address, User, Port_No, password\
                FROM SITE_INFO, ALLOCATION_MAPPING\
                WHERE Frag_Id={} AND\
                ALLOCATION_MAPPING.Host_Name=SITE_INFO.Host_Name".format(frag_id)
        
        conn_creds = self.execute_query(query)
        host_name, ip_address, user, port_no, password = conn_creds[0]
        remote_conn = self.connect(ip_address, user, password)
        remote_cur = remote_conn.cursor()

        return remote_conn, remote_cur
    
    def insert_values(self, result, frag_id, remote_conn, remote_cur, col_names):
        # -------------- removing previous data, if any ----------------------------
        query = 'DELETE FROM {};'.format(self.fragID_fragName_dict[frag_id])
        print('query:', query)
        self.execute_remote_query(query, remote_cur)

        # --------------- inserting new data -----------------------------------------
        query = ''
        print('---------- result here ----------', result)
        for row in result:
            query = query + "INSERT INTO {} ({}) VALUES{};".format(self.fragID_fragName_dict[frag_id], col_names, row)
            # query = 'SELECT * FROM {};'.format(self.fragID_fragName_dict[frag_id])
        print('query:', query, 'type(cur):', type(remote_cur))
        output = self.execute_remote_query(query, remote_cur)
        print(output)

        remote_conn.commit()
        remote_conn.close()
    
    def get_all_fragments(self, table_name):
        print('here, table_name:', table_name)
        query = "SELECT *\
            FROM FRAGMENTS, APPLICATION_TABLE\
            WHERE Table_Name='{}' AND\
            APPLICATION_TABLE.Table_Id=FRAGMENTS.Table_Id".format(table_name)
        
        # print('q:', query)
        
        return self.execute_query(query)
    
    def get_predicate_condition(self, frag_id):
        query = "SELECT Predicate_Cond\
                FROM HF_PREDICATES, PREDICATES\
                WHERE Frag_Id={}\
                AND HF_PREDICATES.Predicate_Id=PREDICATES.Predicate_Id;".format(frag_id)

        return self.execute_query(query)
    
    def get_column_names(self, frag_id):
        query = "SELECT Column_Name\
                FROM VF_COLUMNS\
                WHERE Frag_Id={}".format(frag_id)
        
        return self.execute_query(query)
    
    def get_table_col_names(self, table_name):
        query = "SELECT Column_Name\
                FROM COLUMNS\
                WHERE COLUMNS.Table_Id = (SELECT Table_Id\
                                        FROM APPLICATION_TABLE\
                                        WHERE Table_Name='{}'\
                                        );".format(table_name)
        result = self.execute_query(query)
        return result
    
    
    def populate_table(self, table_name):
        all_fragments = self.get_all_fragments(table_name)
        sql_table_cur = self.get_sql_table_from_df(self.df, create_engine, table_name)
        print('all_frags:', all_fragments)

        # ------------- fetch col_names for the given table ------------------
        
        col_name_lst = self.get_table_col_names(table_name)
        columns = ','.join(col_name[0] for col_name in col_name_lst)
        print('columns:', columns)

        for fragment in all_fragments:
            frag_id = fragment[0]
            frag_name = fragment[2]
            frag_type = fragment[4]
            self.fragID_fragName_dict[frag_id] = frag_name

            if(frag_type == 'HF'):

                fragID_predicate_dict = {}
                fragID_predicate_lst_dict = {}
                result = self.get_predicate_condition(frag_id)

                for row in result:
                    predicate = row[0]
                    if(frag_id not in fragID_predicate_lst_dict):
                        fragID_predicate_lst_dict[frag_id] = []
                    fragID_predicate_lst_dict[frag_id].append(predicate)


                for frag_id, minterms in fragID_predicate_lst_dict.items():
                    predicate = ' and '.join(minterm for minterm in minterms)
                    fragID_predicate_dict[frag_id] = predicate
                
                
                for frag_id, predicate in fragID_predicate_dict.items():
                    query = "SELECT {} FROM {} WHERE {};".format(columns, table_name, predicate)
                    result = sql_table_cur.execute(query).fetchall()
                    print('######### Frag {} #########'.format(frag_id))
                    # writing this to csv file, may be used in dhf
                    header = []
                    for col in col_name_lst:
                        header.append(col[0])
                    # print('col_list:', header)
                    with open(frag_name+'.csv', 'w') as fp:
                        # creating a csv writer object 
                        csvwriter = csv.writer(fp) 
                            
                        # writing the fields 
                        csvwriter.writerow(header) 
                            
                        # writing the data rows 
                        csvwriter.writerows(result)
                    print(result)
                    if(len(result)==0):
                        continue
                    remote_conn, remote_cur = self.get_remote_creds(frag_id)
                    self.insert_values(result, frag_id, remote_conn, remote_cur, columns)
                    
                # self.conn.close()

            elif(frag_type == 'VF'):
                col_name_lst = self.get_column_names(frag_id)
                # print('col_list:', col_name_lst)
                columns = ','.join(col_name[0] for col_name in col_name_lst)
                query = 'SELECT '+columns+' FROM {};'.format(table_name)

                result = sql_table_cur.execute(query).fetchall()
                print('######### Frag {} #########'.format(frag_id))
                print(result)
                if(len(result)==0):
                    continue
                remote_conn, remote_cur = self.get_remote_creds(frag_id)
                # print('type_conn:{} type_cur:{}'.format(type(remote_conn), type(remote_cur)))
                self.insert_values(result, frag_id, remote_conn, remote_cur, columns)
                
            elif(frag_type == 'DHF'):
                # -------- get parent_frag_id for this fragment --------------
                query = "SELECT F1.Parent_Id, F2.Frag_Type\
                        FROM FRAGMENTS F1, FRAGMENTS F2\
                        WHERE F1.Frag_Id={} AND F1.Parent_Id!='NULL' AND F2.Frag_Id=F1.Parent_Id;".format(frag_id)
                
                result = self.execute_query(query)
                # print('--------- printing result ---------')
                parent_frag_id = None
                parent_frag_type = None
                for res in result:
                    # print(res)
                    parent_frag_id = res[0]
                    parent_frag_type = res[1]
                
                if(parent_frag_id==None or parent_frag_type==None):
                    print('bye bye')
                    return
                
                # ------- get table name from frag_name -------
                query = "SELECT F.Frag_Name\
                        FROM FRAGMENTS F\
                        WHERE F.Frag_Id={};".format(parent_frag_id)
                
                result = self.execute_query(query)
                for res in result:
                    # parent_table_name = res[0]
                    parent_frag_name = res[0]
                
                
                # if(parent_frag_type=='HF'):
                #     parent_col_names = self.get_table_col_names(parent_table_name)
                
                # elif(parent_frag_type=='VF'):
                #     parent_col_names = self.get_column_names(parent_frag_id)
                
                # parent_cols = ','.join(col_name[0] for col_name in parent_col_names)
                # print('parent_frag_name:{} type:{}'.format(parent_frag_name, parent_frag_type))

                # do natural join of EMP1 and csv table
                df_parent = pd.DataFrame(pd.read_csv (parent_frag_name+'.csv'))
                df_child = self.df
                df_res = df_parent.merge(df_child, how='inner')
                # for row in df_res:
                #     print(row)
                query = "SELECT {} FROM {};".format(columns, table_name)
                # print('query:', query)
                temp_sql_table_cur = self.get_sql_table_from_df(df_res, create_engine, table_name)
                result = temp_sql_table_cur.execute(query).fetchall()
                if(len(result) == 0):
                    continue
                for row in result:
                    print('row:',row)
                
                remote_conn, remote_cur = self.get_remote_creds(frag_id)
                # result, frag_id, remote_conn, remote_cur, col_names
                # print('------------ sending res:', result)
                self.insert_values(result, frag_id, remote_conn, remote_cur, columns)

               