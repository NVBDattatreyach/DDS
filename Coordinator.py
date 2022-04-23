import threading
import logging
import socket
import time
import requests
from threading import *
import pymysql
from pymysql.constants import CLIENT


obj = Semaphore(1)
participant_finished = 0
vote_commit = 0


class thread(threading.Thread):

    def __init__(self, thread_name, site, msg):
        threading.Thread.__init__(self)
        # Create a socket object
        # self.s = socket.socket() 
        
        self.thread_name = thread_name
        self.site = site
        self.msg = msg
        self.ip_address_mapping={"CP5":"10.3.5.211","CP6":"10.3.5.208","CP7":"10.3.5.204","CP8":"10.3.5.205"}

        if(self.thread_name.endswith('_0')):
            # Create and configure logger
            self.file_handler = open('newfile.txt', 'w')

    def send_to_participant(self, site, queries):
        for query in queries:
            # print('query to participant:', query)
            url="http://{}:7000/to_participant".format(self.ip_address_mapping[site])
            r=requests.post(url,json={'query':query})
            # print('status:', r.json()['status'])
            return r.json()['status']    

    # helper function to execute the threads
    def run(self):
        global obj 
        global participant_finished
        global vote_commit
        # print('connected to participant:{} ip:{} port:{}'.format(self.thread_name, self.participant[0], int(self.participant[1])))

        # connect to participant
        # self.s.connect((self.participant[0], int(self.participant[1])))

        
        # log into file
        if(self.thread_name.endswith('_0')):
            self.file_handler.write("begin_commit\n")

        # send 'prepare' message to participant
        # self.s.send(self.msg.encode())
        response = self.send_to_participant(self.site, [self.msg])
        print('{} data sent: {}'.format(self.thread_name, self.msg))

        # get reply from participant
        # recv_data = self.s.recv(1024).decode()
        print('{} data recvd: {}'.format(self.thread_name, response))

        # wait till all the threads complete till here
        obj.acquire()
        participant_finished += 1
        if(response == 'VOTE-COMMIT'):
            vote_commit += 1
        obj.release()
        
        a = 0
        while a<5:
            if(participant_finished == 2):
                break
            print('{} sleeping. finished: {}'.format(self.thread_name, participant_finished))
            time.sleep(1)
            a += 1
        
        print("{} out of loop".format(self.thread_name))
        # check for the votes
        if(vote_commit == 2):
            self.msg = 'GLOBAL-COMMIT'   
        else:
            self.msg = 'GLOBAL-ABORT'
        # self.s.send(msg.encode())
        print('{} data sent: {}'.format(self.thread_name, self.msg))
        response = self.send_to_participant(self.site, [self.msg])

        # log into file
        if(self.thread_name.endswith('_0')):
            self.file_handler.write(self.msg+'\n')

        # get 'ACK' from participants
        # recv_data = self.s.recv(1024).decode()
        print('{} data recvd: {}'.format(self.thread_name, response))


        # log into file
        if(self.thread_name.endswith('_0')):
            self.file_handler.write('end_transaction')

        # self.s.close()
 
class Coordinator:

    def __init__(self, where_clause):
        self.ip_address_mapping={"CP5":"10.3.5.211","CP6":"10.3.5.208","CP7":"10.3.5.204","CP8":"10.3.5.205"}
        self.conn=self.connect('10.3.5.211', 'xavier', 'xmen123')
        self.cur=self.conn.cursor()
        self.host_name_list = []
        self.another_host_name = []
        self.leaf_info = []
        self.where_clause = where_clause[0].value
        self.hostname_fragname_map = {}
        # NUM_PARTICIPANTS = 2

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


    def query(self, site, queries):
        for query in queries:
            url="http://{}:7000/execute_query".format(self.ip_address_mapping[site])
            r=requests.post(url,json={'query':query})
            return r.json()['status']
            


    def get_participants(self, root):
        if(root == None):
            return
        if(len(root.children) == 0):
            frag_id, frag_name = root.data.split()[0], root.data.split()[1]
            
            query = 'SELECT Host_name\
                    FROM ALLOCATION_MAPPING\
                    WHERE Frag_Id={}'.format(frag_id)
            result = self.execute_query(query)
            hostname = result[0][0]

            self.leaf_info.append((frag_id, frag_name, hostname))
            self.hostname_fragname_map[hostname] = frag_name


            condition = self.where_clause
            initial_query = "SELECT COUNT(*)\
                            FROM {}\
                            WHERE {};".format(frag_name, condition)
            print('initial query:', initial_query)
            print('frag_name:{} hostname:{}'.format(frag_name, hostname))
            response = self.query(hostname, [initial_query])
            print('response of initial query:', response)
            # print('status:', r.json()['status'])
            if(response[0][0]>0):
                is_participant = True
            else:
                is_participant = False

            if(is_participant == True):
                self.host_name_list.append(hostname)

            # print('frag_id:{} host:{}'.format(frag_id, result))
        for child in root.children:
            self.get_participants(child)
    
    
    def extract_attr_from_set_clause(self, set_clause):
        attr_list = []
        for item in set_clause:
            lhs = item.split('=')[0]
            if('.' in lhs):
                attr = lhs.split('.')[1]
                attr_list.append(attr)
        
        return attr_list
    
    def get_table_columns(self, from_clause):
        table_name = from_clause[0]
        query = "SELECT Column_Name\
                FROM COLUMNS\
                WHERE COLUMNS.Table_Id = (SELECT Table_Id\
                                        FROM APPLICATION_TABLE\
                                        WHERE Table_Name='{}'\
                                        );".format(table_name)
        result = self.execute_query(query)
        col_names = [col[0] for col in result]
        return col_names


    def get_another_participant(self, set_clause, from_clause):
        self.table_columns = self.get_table_columns(from_clause)
        # print('table cols:', self.table_columns)

        
        # get predicate conditions from all fragments, in advance
        hostname_predicate_map = {}
        for info in self.leaf_info:
            frag_id, frag_name, hostname = info
            query = "SELECT Predicate_Cond\
                    FROM HF_PREDICATES, PREDICATES\
                    WHERE Frag_Id={}\
                    AND HF_PREDICATES.Predicate_Id=PREDICATES.Predicate_Id;".format(frag_id)

            result = self.execute_query(query)
            
            # get the list of attributes used in the predicate condition
            predicate_conditions = {}

            for row in result:
                predicate = row[0]
                if('!=' in predicate):
                    attribute, value = predicate.split('!=')
                    sign = '!='
                elif('=' in predicate):
                    attribute, value = predicate.split('=')
                    sign = '='
                elif('>=' in predicate):
                    attribute, value = predicate.split('>=')
                    sign = '>='
                elif('<=' in predicate):
                    attribute, value = predicate.split('<=')
                    sign = '<='
                elif('>' in predicate):
                    attribute, value = predicate.split('>')
                    sign = '>'
                elif('<' in predicate):
                    attribute, value = predicate.split('<')
                    sign = '<'
                predicate_conditions[attribute] = (sign, value)
            
            hostname_predicate_map[hostname] = predicate_conditions

        # extract predicate column names
        for hostname in hostname_predicate_map.keys():
            predicate_columns = list(hostname_predicate_map[hostname].keys())
            # print('predicate cols:', predicate_columns)
            predicate_columns_str = ','.join(col for col in predicate_columns)
            break

        self.host_pairs = []
        for hosts in self.host_name_list:
            # get updated set clause, where table_name before attr name is removed 
            cur_hostname = hosts
            cur_fragname = self.hostname_fragname_map[cur_hostname]
            
            # now get values of these columns from frag_name which satisfy the condition
            self.table_columns_str = ','.join(col for col in self.table_columns)
            query = 'SELECT {} FROM {} WHERE {};'.format(self.table_columns_str, cur_fragname, self.where_clause)
            print('query is:', query)
            # result = self.execute_query(query)
            result = self.query(cur_hostname, [query])
            print('result recv from query:', result)

            # now create attr_name => value map for the above result
            self.attr_value_map = {}
            for row in result:
                for col, value in zip(self.table_columns, row):
                    self.attr_value_map[col] = value

                for item in set_clause:
                    item, value = item.split('=')
                    if('.' in item):
                        item = item.split('.')[1]
                    self.attr_value_map[item] = value
                break

            # for current fragment, go to all the frags and check for the suitable site, based on predicate 
            target_hostname = None
            # print('attr_val_map:', self.attr_value_map)
            for hostname, predicate_details in hostname_predicate_map.items():
                cnt = 0
                for attribute_name, attr_info in predicate_details.items():
                    sign = attr_info[0]
                    val = attr_info[1]
                    if(sign == '='):
                        # print('sign:= {} == {}'.format(self.attr_value_map[attribute_name], val))
                        if(self.attr_value_map[attribute_name] == val):
                            cnt += 1
                    elif(sign == '!='):
                        # print('sign:= {} != {}'.format(self.attr_value_map[attribute_name], val))
                        if(self.attr_value_map[attribute_name] != val):
                            cnt += 1
                    elif(sign == '>='):
                        if(self.attr_value_map[attr_name] >= val):
                            cnt += 1
                    elif(sign == '<='):
                        if(self.attr_value_map[attr_name] <= val):
                            cnt += 1
                    elif(sign == '>'):
                        if(self.attr_value_map[attr_name] > val):
                            cnt += 1
                    elif(sign == '<'):
                        if(self.attr_value_map[attr_name] < val):
                            cnt += 1
                # print('cnt:{} len_pred_details:{}'.format(cnt, len(predicate_details)))
                if(cnt == len(predicate_details)):
                    target_hostname = hostname
                    target_fragname = self.hostname_fragname_map[target_hostname]
                    break
            self.host_pairs.append((cur_hostname, cur_fragname, target_hostname, target_fragname))
            break
               
        print('all pairs:', self.host_pairs)
    
    def get_ip_port(self, hostname):
        query = "SELECT Ip_address, Port_No FROM SITE_INFO WHERE Host_Name='{}'".format(hostname)
        print('query:', query)
        result = self.execute_query(query)
        print('result:', result)
        return result
    
    
    # start the 2PC protocol
    def Two_PC(self):

        delete_from_hostname = self.host_pairs[0][0]
        delete_from_fragname = self.host_pairs[0][1]
        insert_into_hostname = self.host_pairs[0][2]
        insert_into_fragname = self.host_pairs[0][3]

        # prepare the query to be sent
        delete_query = 'DELETE FROM {} WHERE {};'.format(delete_from_fragname, self.where_clause)
        insert_query = 'INSERT INTO {} ({}) VALUES{};'.format(insert_into_fragname, self.table_columns_str, tuple(self.attr_value_map.values()))

        # get the ip and port number
        # delete_from_ip, delete_from_port = self.get_ip_port(delete_from_hostname)[0]
        # insert_into_ip, insert_into_port = self.get_ip_port(insert_into_hostname)[0]

        # print('msg to A: {} {}: {}'.format(delete_from_ip, delete_from_port, delete_query))
        # print('msg to B: {} {}: {}'.format(insert_into_ip, insert_into_port, insert_query))

        participants_thread = []

        # create thread for delete operation
        msg = 'PREPARE;'+ delete_query
        participants_thread.append(thread(delete_from_hostname+'_0', delete_from_hostname, msg))
        participants_thread[-1].start()

        # create thread for insert operation
        msg = 'PREPARE;'+insert_query
        participants_thread.append(thread(insert_into_hostname+'_1', insert_into_hostname, msg))
        participants_thread[-1].start()

        # join all threads
        for t in participants_thread:
            print('{} joined'.format(t))
            t.join()

        
'''     
participants_thread = []

PARTICIPANTS = [('127.0.0.1', '1234'), ('127.0.0.1', '1235'), ('127.0.0.1', '24')]


# start the transaction


for i in range(2):
    P_name = 'P'+str(i)
    participants_thread.append(thread(P_name, PARTICIPANTS[i]))
    participants_thread[-1].start()

# join all threads
for t in participants_thread:
    print('{} joined'.format(t))
    t.join()
# close the connection
# s.close() 
print("Exit")
'''