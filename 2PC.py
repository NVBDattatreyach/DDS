from flask import Flask
from flask import jsonify
from flask import request
import pymysql
import os
import uuid
import pandas as pd
import json

conn=pymysql.connect(
    host="localhost",
    password='iiit123',
    database='XMEN'
        )

app=Flask(__name__)

def execute(query,conn):
    return pd.read_sql_query(query,conn)
    


@app.route('/',methods=['GET','POST'])
def hello():
    return 'hello world!'


@app.route('/query',methods=['POST'])
def run_query():
    """
        executes query and saves it to a file
    """
    data=request.get_json()
    query=data['query']    
    result=execute(query,conn)
    result_file='/home/user/xmen/temp/'+str(uuid.uuid4())+".csv"
    result.to_csv(result_file,index=False)
    
    return jsonify({'path':result_file})
    
    
@app.route('/send_to_peer',methods=['POST'])
def send_to_peer():
    """
        sends given file to the remote peer
    """
    data=request.get_json()
    local_path=data['local_path']
    remote_user=data['remote_user']
    remote_ip=data['remote_ip']
    remote_path=data['remote_path']
    os.system('scp {} {}@{}:{}'.format(local_path,remote_user,remote_ip,remote_path))
    return jsonify({'status':'OK'})
@app.route('/create_view',methods=['POST'])
def create_view():
    data=request.get_json()
    query=data['query']
    print(query)
    cursor=conn.cursor()
    cursor.execute(query)
    conn.commit()
    return jsonify({'status':'OK'})
@app.route('/get_profile',methods=['POST'])
def get_profile():
    """
    return profile of the given table name
    """
    data=request.get_json()
    table_name=data['table_name']
    f=open('profiles/{}.json'.format(table_name),'r')
    data=json.load(f)
    return jsonify(data)
@app.route('/create_dump',methods=['POST'])
def create_dump():
    data=request.get_json()
    query=data['query']
    table_name=data['file_name']
    file_name=table_name+".sql"
    cursor=conn.cursor()
    cursor.execute(query)
    os.system('mysqldump -uuser -piiit123 XMEN {} > /home/user/xmen/temp/{}'.format(table_name,file_name))
    return jsonify({'status':'OK'})

@app.route('/create_direct_dump',methods=['POST'])
def create_direct_dump():
    data=request.get_json()
    table_name=data['table_name']
    print(table_name)
    os.system('mysqldump -uuser -piiit123 XMEN {} > /home/user/xmen/temp/{}.sql'.format(table_name,table_name))
    return jsonify({'status':'OK'})

@app.route('/load_dump',methods=['POST'])
def load_dump():
    data=request.get_json()
    local_path=data['filepath']
    print(local_path)
    os.system('mysql -uuser -piiit123 XMEN < {}'.format(local_path))
    return jsonify({'status':'OK'})

@app.route('/execute_query',methods=['POST'])
def execute_query():
    data=request.get_json()
    query=data['query']
    cursor=conn.cursor()
    cursor.execute(query)
    result = cursor.fetchall()
    print('execute_query_result:', result)
    # print(local_path)
    # os.system('mysql -uuser -piiit123 XMEN < {}'.format(local_path))
    if(query.startswith('UPDATE')):
        conn.commit()
    return jsonify({'status':result})

log_file = open('newfile.txt', 'w')
@app.route('/to_participant',methods=['POST'])
def process_2PC_request():
    data=request.get_json()
    query=data['query']
    print('query:', query)
    query_parts = query.split(';', 1)
    cursor=conn.cursor()
    global log_file
    if(query_parts[0] == 'PREPARE'):
        log_file = open('newfile.txt', 'w')
        query = query_parts[1]
        print('msg:{} query:{}'.format(query_parts[0], query))  
        try:
            cursor.execute(query)
            log_file.write('READY\n')
            # result = cursor.fetchall()
        except:
            log_file.write('ABORT\n')
            return jsonify({'status': 'VOTE-ABORT'})
        return jsonify({'status': 'VOTE-COMMIT'})
    
    if(query_parts[0] == 'GLOBAL-COMMIT'):
        log_file.write('COMMIT\n')
        conn.commit()
    else:
        log_file.write('ABORT\n')
    log_file.close()
    return jsonify({'status': 'ACK'})
    


if __name__=='__main__':
    app.run(host='0.0.0.0',port=7000)
