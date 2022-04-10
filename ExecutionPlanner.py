'''
This file contains code to prepare an execution plan
It will be format: site# --> query to be executed

'''



import pymysql
from pymysql.constants import CLIENT


class ExecutionPlanner:

    def __init__(self):
        self. fragname_fraginfo_map = {}
        self.execution_plan = []
        self.alias_name_id = 0
        self.alias_query_map = []
        self.conn=self.connect('10.3.5.211', 'xavier', 'xmen123')
        print('############## CONNECTED TO SERVER ##################')
        print()
        print()
        self.cur=self.conn.cursor()
    
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
    

    def get_fragid_from_fragname(self, frag_name):
        query = "SELECT Frag_Id\
                FROM FRAGMENTS\
                WHERE Frag_Name='{}'".format(frag_name)
        
        print('query:', query)
        result = self.execute_query(query)
        # print('res:', result)
        return result[0][0]
    
    def get_hostname(self, frag_id):
        query = "SELECT SITE_INFO.Host_Name, Ip_address, User, Port_No, password\
                FROM SITE_INFO, ALLOCATION_MAPPING\
                WHERE Frag_Id={} AND\
                ALLOCATION_MAPPING.Host_Name=SITE_INFO.Host_Name".format(frag_id)
        
        conn_creds = self.execute_query(query)
        host_name, ip_address, user, port_no, password = conn_creds[0]
        return host_name
        
    # ------- This method will do postorder traversal and just return the (accumulated) node data ------
    def postorder(self, root, hostname):
        if(root == None):
            return
        child_queries = []

        # ------------------ leaf node case ----------------------
        if(len(root.children) == 0):
            table_name, frag_name = root.data.split()[-1], root.data.split()[1]
            self.fragname_fraginfo_map[table_name] = root.data.split()
            child_query = frag_name
            # print('at leaf:', child_query)

            # finding hostname for this fragment
            frag_Id = self.get_fragid_from_fragname(frag_name)
            hostname = self.get_hostname(frag_Id)
            return [child_query, hostname]
            # return child_query
            
        for child in root.children:
            # res = self.postorder(child, hostname)
            # print('res:', res)
            child_query, hostname = self.postorder(child, hostname)
            if(child_query==None or hostname==None):
                continue
            # print('child_query here:{} at {}'.format(child_query, hostname))
            child_queries.append({child_query: hostname})
        
        # ----------------------- root belong to a direct query -----------------------
        if(len(child_queries) == 1):
            # print('root:', root.data)

            for key, val in child_queries[0].items():
                query, hostname = key, val
            # print('query:{} hostname:{}'.format(query, hostname))

            if(root.data.startswith('project')):
                child_node = root.children[0]
                if(len(child_node.children) == 0):
                    query = root.data.replace("project", "SELECT") + ' FROM ' + query
                    if(not root.parent.data.startswith('select')):
                        query = query + ' As t{}'.format(self.alias_name_id)
                        self.alias_name_id += 1
                    self.execution_plan.append({hostname: query})

    
            elif(root.data.startswith('select')):
                
                for table_name, table_info in self.fragname_fraginfo_map.items():
                    if(table_name in root.data):
                        frag_name = table_info[1]
                        root.data = root.data.replace(table_name, frag_name)
                        
                        # print('lhs:{} rhs:{}'.format(child_queries[0], root.data.split(' ', 1)[1]))          
                        query = query + ' WHERE ' + root.data.split(' ', 1)[1]+' As t{}'.format(self.alias_name_id)
                        self.alias_name_id += 1

                        # find site on which this direct query should run
                        frag_Id = self.get_fragid_from_fragname(frag_name)
                        hostname = self.get_hostname(frag_Id)
                        self.execution_plan.pop()
                        self.execution_plan.append({hostname: query})
                        break
            
           
            # print('with one child:', query)
            return query, hostname


        #------------------------ root belongs to a join/union query -------------------------
        else:
            # print('root:', root.data)
            if(root.data.startswith('union')):
                query = ''
                for child_query in child_queries:
                    for q, h in child_query.items():
                        table_alias = q.split()[-1]
                        query = query + ' union ' + table_alias
                        host = h
                
                query = query + ' As t{}'.format(self.alias_name_id)
                self.alias_name_id += 1
                query = query.split('union', 1)[1].strip()
                print('union query:', query)
                self.execution_plan.append({host: query})
                return query, host
            
            elif(root.data.startswith('join') or root.data.startswith('vf join')):
                query = ''
                for child_query in child_queries:
                    for q, h in child_query.items():
                        table_alias = q.split()[-1]
                        query = query + ' join ' + table_alias
                        host = h
                # -------------- finding the joining attribute ---------------------
                if(root.data.startswith('vf join')):
                    join_condition = root.data.split(' ')[-1]
                else:
                    join_condition = root.data.split(' ', 1)[1]
                query = query + ' on ' + join_condition
                
                query = query + ' As t{}'.format(self.alias_name_id)
                self.alias_name_id += 1
                query = query.split('join', 1)[1].strip()
                print('join query:', query)
                self.execution_plan.append({host: query})
                return query, host
            return None, None
            
    
    def prepare_execution_plan(self, root):
        self.postorder(root, '.')
        print()
        print()
        print('-------------- exec plan -------------')
        for plan in self.execution_plan:
            for hostname, query in plan.items():
                print('{} ===> {}'.format(hostname, query))
        
        exec_plan = {}
        host = None
        for plan in self.execution_plan:
            for hostname, query in plan.items():
                if(host != hostname):
                    exec_plan[hostname] = [query]
                    if(host!=None):
                        shipping_cmd = 'ship to {}'.format(hostname)
                        exec_plan[host].append(shipping_cmd)
                    host = hostname
                else:
                    exec_plan[hostname].append(query)
        
        print()
        print()
        print('----------- updated exec plan -----------')
        for hostname, query in exec_plan.items():
            print('{} ===> {}'.format(hostname, query))
        
        # merging same site join queries to one sql query
        for hostname, queries in exec_plan.items():
            select_list = []
            from_list = []
            where_list = []
            alias_list = []

            for idx, query in enumerate(queries):

                alias_name = (query.split())[-1]
                alias_list.append(alias_name)

                if(query.startswith('SELECT')):
                    query = query.split('SELECT ')[1]
                    select, query = query.split(' FROM ')
                    if('WHERE' in query):
                        fromm, query = query.split(' WHERE ')
                        where, query = query.split(' As ')
                        where_list.append(where)
                    else:
                        fromm, query = query.split(' As ')
                    from_list.append(fromm)
            
                    for attr in select.split(','):
                        var_name = fromm+'.'+attr
                        select_list.append(var_name)
                
                elif('join' in query):
                    query_split = query.split()
                    if(query_split[0] in alias_list and query_split[2] in alias_list):
                        select_attrs = ','.join(attr for attr in select_list)
                        from_tables = ','.join(table for table in from_list)
                        join_condition = (query.split(' on ')[1]).split(' As ')[0]
                        where_list.append(join_condition)
                        where_condn = ' and '.join(condn for condn in where_list)
                        
                        query = 'SELECT '+select_attrs+' FROM '+from_tables+' WHERE '+where_condn + ' As '+alias_name

                        del exec_plan[hostname][idx-1]
                        exec_plan[hostname][idx-1] = query
                        del exec_plan[hostname][idx-2]

                    # exec_plan[hostname].append(query)
        
        print()
        print()
        print('----------- updated exec plan -----------')
        for hostname, query in exec_plan.items():
            print('{} ===> {}'.format(hostname, query))
        
        return exec_plan
