# from ReadCSV import df
import pymysql
from pymysql.constants import CLIENT




class TableHandler:

    def __init__(self):
        self.conn=self.connect('10.3.5.211', 'xavier', 'xmen123')
        print('############## CONNECTED TO SERVER ##################')
        print()
        print()
        self.cur=self.conn.cursor()
        self.fragID_fragName_dict = {}
        self.table_attr_map = {}
    
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
    
    
    def get_all_fragments(self, table_name):
        query = "SELECT *\
            FROM FRAGMENTS, APPLICATION_TABLE\
            WHERE Table_Name='{}' AND\
            APPLICATION_TABLE.Table_Id=FRAGMENTS.Table_Id;".format(table_name)
                
        return self.execute_query(query)
    
    
    def get_column_names(self, frag_id):
        query = "SELECT Column_Name\
                FROM VF_COLUMNS\
                WHERE Frag_Id={}".format(frag_id)
        
        return self.execute_query(query)
    
    def get_attributes(self):
        query = "SELECT Table_Name FROM APPLICATION_TABLE;"
        table_names = self.execute_query(query)
        for table_name in table_names:
            print('table name:', table_name[0])
            try:
                self.get_attributes_for_table(table_name[0])
            except:
                print('{} d.n.e', table_name[0])
    
    def get_attributes_for_table(self, table_name):
        all_fragments = self.get_all_fragments(table_name)
        frag_type = all_fragments[0][4]
        frag_id = all_fragments[0][0]
        frag_name = all_fragments[0][2]
        
        if(frag_type == 'HF' or frag_type == 'DHF' or frag_type == "NA"):
            remote_conn, remote_cur = self.get_remote_creds(frag_id)
            if(frag_type == "NA"):
                frag_name = table_name
            query = 'SELECT * FROM {};'.format(frag_name)
            
            data = remote_cur.execute(query)
            table_contents = remote_cur.fetchall()

            num_fields = len(remote_cur.description)
            field_names = [i[0] for i in remote_cur.description]
            if(table_name not in self.table_attr_map):
                self.table_attr_map[table_name] = []
            self.table_attr_map[table_name] = field_names
            remote_conn.commit()
            remote_conn.close()
            # self.conn.close()

        if(frag_type == 'VF'):
            for fragment in all_fragments:
                frag_id = fragment[0]
                col_name_lst = self.get_column_names(frag_id)
                print('col_list:', col_name_lst)
                if(table_name not in self.table_attr_map):
                    self.table_attr_map[table_name] = []
                for col_name in col_name_lst:
                    self.table_attr_map[table_name].append(col_name)
        
        # else:


table_handler = TableHandler()
table_handler.get_attributes()
    