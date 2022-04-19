from flask import Flask
from flask import jsonify
from flask import request
import pymysql
import os
import uuid
import pandas as pd
import json



app=Flask(__name__)

def get_connection():
    conn=pymysql.connect(
        host="localhost",
        password='iiit123',
        database='XMEN'
            )
    return conn
    
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
    conn=get_connection()
    data=request.get_json()
    query=data['query']
    print(query)
    cursor=conn.cursor()
    cursor.execute(query)
    conn.commit()
    conn.close()
    return jsonify({'status':'OK'})

@app.route('/create_dump',methods=['POST'])
def create_dump():
    conn=get_connection()
    data=request.get_json()
    query=data['query']
    table_name=data['file_name']
    file_name=table_name+".sql"
    cursor=conn.cursor()
    cursor.execute(query)
    conn.close()
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
    x=os.system('mysql -uuser -piiit123 XMEN < {}'.format(local_path))
    if(x==256):
        print("uncompatible")
        os.system("sed -i 's/utf8mb4_0900_ai_ci/utf8_general_ci/g' {}".format(local_path))
        os.system("sed -i 's/CHARSET=utf8mb4/CHARSET=utf8/g' {}".format(local_path))
        x=os.system('mysql -uuser -piiit123 XMEN < {}'.format(local_path))
        if(x!=256):
            print("compatible")
    return jsonify({'status':'OK'})

@app.route('/drop_table',methods=['POST'])
def drop_table():
    conn=get_connection()
    data=request.get_json()
    tables=data['tables']
    cursor=conn.cursor()
    query="DROP TABLE {}".format(tables)
    cursor.execute(query)
    conn.commit()
    conn.close()
    return jsonify({'status':'OK'})
if __name__=='__main__':
    app.run(host='0.0.0.0',port=8000)
