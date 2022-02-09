import pymysql
from pymysql.constants import CLIENT

ip_address=['10.3.5.211','10.3.5.208','10.3.5.204','10.3.5.205']
for address in ip_address:
    print("creating tables at ",address)
    conn=pymysql.connect(
        host=address,
        user='xavier',
        password='xmen123',
        db='XMEN',
        client_flag=CLIENT.MULTI_STATEMENTS,)
    cur=conn.cursor()
    if address=="10.3.5.211":
        cur.execute("""
            CREATE TABLE IF NOT EXISTS EMP1(
                Emp_Id int,
                Dept_Name varchar(255),
                Loc_Id varchar(255),
                Desgn_Id int,
                Reports_To int,
                PRIMARY KEY (Emp_Id)
            );
            CREATE TABLE IF NOT EXISTS DEPARTMENT(
                Dept_Name varchar(255),
                PRIMARY KEY (Dept_Name)
            );
            CREATE TABLE IF NOT EXISTS EMP_DET1(
                Emp_Id int NOT NULL,
                Name varchar(255),
                Age int,
                Gender varchar(255),
                Phone_No varchar(255),
                Email_Id varchar(255)
            );
            CREATE TABLE IF NOT EXISTS PROJECT1(
                Project_Id int,
                Project_Name int NOT NULL,
                Dept_Name varchar(255) NOT NULL,
                PRIMARY KEY (Project_Id)
            );

            CREATE TABLE IF NOT EXISTS WORKS1(
                Emp_Id int NOT NULL,
                Project_Id int NOT NULL
            );
        """)
        conn.commit()
        conn.close()
    elif address=="10.3.5.208":
        cur.execute("""
            CREATE TABLE IF NOT EXISTS EMP2(
                Emp_Id int,
                Dept_Name varchar(255),
                Loc_Id varchar(255),
                Desgn_Id int,
                Reports_To int,
                PRIMARY KEY (Emp_Id)
                
            );
            CREATE TABLE IF NOT EXISTS LOCATION(
                Loc_Id varchar(255),
                PRIMARY KEY (Loc_Id)
            )
            CREATE TABLE IF NOT EXISTS EMP_DET2(
                Emp_Id int NOT NULL,
                Name varchar(255),
                Age int,
                Gender varchar(255),
                Phone_No varchar(255),
                Email_Id varchar(255)
                
            );
            CREATE TABLE IF NOT EXISTS PROJECT2(
                Project_Id int,
                Budget int NOT NULL,
                PRIMARY KEY (Project_Id)
            );

            CREATE TABLE IF NOT EXISTS WORKS2(
                Emp_Id int NOT NULL,
                Project_Id int NOT NULL
                 
            );

            
        """)
    elif address=="10.3.5.204":
        cur.execute("""
            CREATE TABLE IF NOT EXISTS EMP3(
                Emp_Id int,
                Dept_Name varchar(255),
                Loc_Id varchar(255),
                Desgn_Id int,
                Reports_To int,
                PRIMARY KEY (Emp_Id)
                
            );
            
            CREATE TABLE IF NOT EXISTS EMP_DET3(
                Emp_Id int NOT NULL,
                Name varchar(255),
                Age int,
                Gender varchar(255),
                Phone_No varchar(255),
                Email_Id varchar(255)
                
            );
            CREATE TABLE IF NOT EXISTS PROJECT1(
                Project_Id int,
                Project_Name int NOT NULL,
                Dept_Name varchar(255) NOT NULL,
                PRIMARY KEY (Project_Id)
            );

            CREATE TABLE IF NOT EXISTS WORKS3(
                Emp_Id int NOT NULL,
                Project_Id int NOT NULL
                
            );
            
            CREATE TABLE IF NOT EXISTS DESG(
                Desgn_Id int,
                Desgn_Name varchar(255),
                Salary int,
                PRIMARY KEY (Desgn_Id)
            )
        """)
    else:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS EMP4(
                Emp_Id int,
                Dept_Name varchar(255),
                Loc_Id varchar(255),
                Desgn_Id int,
                Reports_To int,
                PRIMARY KEY (Emp_Id)
                
            );
            CREATE TABLE IF NOT EXISTS EMP_DET4(
                Emp_Id int NOT NULL,
                Name varchar(255),
                Age int,
                Gender varchar(255),
                Phone_No varchar(255),
                Email_Id varchar(255)
                
            );
            
            CREATE TABLE IF NOT EXISTS PROJECT2(
                Project_Id int,
                Budget int NOT NULL,
                PRIMARY KEY (Project_Id)
            );

            CREATE TABLE IF NOT EXISTS WORKS4(
                Emp_Id int NOT NULL,
                Project_Id int NOT NULL
                
            );

            
            
        """)

"""
EMP1,EMP_DET1,DEPT,Project_1,WORKS_ON1,DESGN1


"""