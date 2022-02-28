import pymysql
from pymysql.constants import CLIENT

def exec_query(cur,conn,query):
    cur.execute(query)
    conn.commit()

def create_tables(cur):
    
    cur.execute("SET FOREIGN_KEY_CHECKS=0;")
    cur.execute("DROP TABLE IF EXISTS APPLICATION_TABLE;")
    cur.execute("DROP TABLE IF EXISTS FRAGMENTS;")
    cur.execute("DROP TABLE IF EXISTS SITE_INFO;")
    cur.execute("DROP TABLE IF EXISTS COLUMNS;")
    cur.execute("DROP TABLE IF EXISTS ALLOCATION_MAPPING;")
    cur.execute("DROP TABLE IF EXISTS PREDICATES;")
    cur.execute("DROP TABLE IF EXISTS HF_PREDICATES")
    cur.execute("DROP TABLE IF EXISTS VF_COLUMNS")

    cur.execute("SET FOREIGN_KEY_CHECKS=1;")
    cur.execute("""
    

    CREATE TABLE IF NOT EXISTS APPLICATION_TABLE
    (
        Table_Id int,
        Table_Name varchar(255) NOT NULL,
        PRIMARY KEY(Table_Id)
    );
    CREATE TABLE IF NOT EXISTS FRAGMENTS
    (
        Frag_Id int,
        Table_Id int,
        Frag_Name varchar(255),
        Parent_Id int,
        Frag_Type varchar(255),
        PRIMARY KEY(Frag_Id),
        FOREIGN KEY(Table_Id) REFERENCES APPLICATION_TABLE(Table_Id),
        FOREIGN KEY(Parent_Id) REFERENCES FRAGMENTS(Frag_Id)
    );
    CREATE TABLE IF NOT EXISTS COLUMNS(
        Table_Id int,
        Column_Name varchar(255),
        Column_Datatype varchar(255),
        Key_Attribute boolean,
        PRIMARY KEY (Table_Id,Column_Name),
        FOREIGN KEY (Table_Id) REFERENCES APPLICATION_TABLE(Table_Id)
    );
    CREATE TABLE IF NOT EXISTS PREDICATES
    (
        Predicate_Id int,
        Predicate_Cond varchar(255),
        PRIMARY KEY (Predicate_Id)
    );
    CREATE TABLE IF NOT EXISTS HF_PREDICATES(
        Predicate_Id int NOT NULL,
        Frag_Id int NOT NULL,
        FOREIGN KEY (Predicate_Id) REFERENCES PREDICATES(Predicate_id),
        FOREIGN KEY (Frag_Id) REFERENCES FRAGMENTS(Frag_Id)
    );
    CREATE TABLE IF NOT EXISTS VF_COLUMNS(
        Frag_Id int NOT NULL,
        Table_Id int NOT NULL,
        Column_Name varchar(255) NOT NULL,
        Is_Frgn_Key Boolean,
        Par_Frag_Id int,
        Par_Frag_Column varchar(255),
        Primary KEY (Frag_Id,Column_Name),
        FOREIGN KEY (Frag_Id) REFERENCES FRAGMENTS(Frag_Id),
        FOREIGN KEY (Table_Id,Column_Name) REFERENCES COLUMNS(Table_Id,Column_Name),
        FOREIGN KEY (Par_Frag_Id,Par_Frag_Column) REFERENCES VF_COLUMNS(Frag_Id,Column_Name)
    );
    CREATE TABLE IF NOT EXISTS SITE_INFO(
        Host_Name varchar(255),
        Ip_address varchar(255) UNIQUE NOT NULL,
        User varchar(255) NOT NULL,
        Port_No int NOT NULL,
        password varchar(255) NOT NULL,
        PRIMARY KEY (Host_Name)
    );
    CREATE TABLE IF NOT EXISTS ALLOCATION_MAPPING(
        Frag_Id int NOT NULL,
        Host_Name varchar(255) NOT NULL,
        FOREIGN KEY (Frag_Id) REFERENCES FRAGMENTS(Frag_Id),
        FOREIGN KEY (Host_Name) REFERENCES SITE_INFO(Host_Name)
    );
    
    """)
def connect(ip_address):
    
    
    conn=pymysql.connect(
        host=ip_address,
        user='xavier',
        password='xmen123',
        db='XMEN',
        client_flag=CLIENT.MULTI_STATEMENTS,)
    return conn
        
        
    
    print("inserting into APPLICATION_TABLE table")
def insert(cur):
    cur.execute("""
        INSERT INTO APPLICATION_TABLE VALUES(1,"EMPLOYEE");
        INSERT INTO APPLICATION_TABLE VALUES(2,"DEPARTMENT");
        INSERT INTO APPLICATION_TABLE VALUES(3,"LOCATION");
        INSERT INTO APPLICATION_TABLE VALUES(4,"DESIGNATION");
        INSERT INTO APPLICATION_TABLE VALUES(5,"PROJECT");
        INSERT INTO APPLICATION_TABLE VALUES(6,"EMPLOYEE_DETAILS");
        INSERT INTO APPLICATION_TABLE VALUES(7,"WORKS_ON");
    """
    )
    print("inserting into COLUMNS table")
    cur.execute("""
        INSERT INTO COLUMNS VALUES(1,'Emp_Id','int',true);
        INSERT INTO COLUMNS VALUES(1,'Dept_Name','varchar(255)',false);
        INSERT INTO COLUMNS VALUES(1,'Loc_Id','varchar(255)',false);
        INSERT INTO COLUMNS VALUES(1,'Desgn_Id','int',false);
        INSERT INTO COLUMNS VALUES(1,'Reports_To','int',false);
        INSERT INTO COLUMNS VALUES(2,'Dept_Name','varchar(255)',true);
        INSERT INTO COLUMNS VALUES(3,'Loc_Id','varchar(255)',true);
        INSERT INTO COLUMNS VALUES(4,'Desgn_Id','int',true);
        INSERT INTO COLUMNS VALUES(4,'Desgn_Name','varchar(255)',false);
        INSERT INTO COLUMNS VALUES(4,'Salary','int',false);
        INSERT INTO COLUMNS VALUES(5,'Project_Id','int',true);
        INSERT INTO COLUMNS VALUES(5,'Project_Name','varchar(255)',false);
        INSERT INTO COLUMNS VALUES(5,'Budget','int',false);
        INSERT INTO COLUMNS VALUES(5,'Dept_Name','varchar(255)',false);
        INSERT INTO COLUMNS VALUES(6,'Emp_Id','int',true);
        INSERT INTO COLUMNS VALUES(6,'Name','varchar(255)',false);
        INSERT INTO COLUMNS VALUES(6,'Age','int',false);
        INSERT INTO COLUMNS VALUES(6,'Gender','varchar(255)',false);
        INSERT INTO COLUMNS VALUES(6,'Phone_No','varchar(255)',false);
        INSERT INTO COLUMNS VALUES(6,'Email_Id','varchar(255)',false);
        INSERT INTO COLUMNS VALUES(7,'Project_Id','int',true);
        INSERT INTO COLUMNS VALUES(7,'Emp_Id','int',false);
    """)
    
    print("inserting into SITE_INFO table")
    conn.commit()
    cur.execute("""
        INSERT INTO SITE_INFO VALUES('CP5','10.3.5.211','user',22,'iiit123');
        INSERT INTO SITE_INFO VALUES('CP6','10.3.5.208','user',22,'iiit123');
        INSERT INTO SITE_INFO VALUES('CP7','10.3.5.204','user',22,'iiit123');
        INSERT INTO SITE_INFO VALUES('CP8','10.3.5.205','user',22,'iiit123');
    """)

    print("inserting into predicates table")
    cur.execute("""
        INSERT INTO PREDICATES VALUES(1,"Dept_Name='SALES'");
        INSERT INTO PREDICATES VALUES(2,"Dept_Name!='SALES'");
        INSERT INTO PREDICATES VALUES(3,"Dept_Name='ADMIN'");
        INSERT INTO PREDICATES VALUES(4,"Dept_Name!='ADMIN'");
        INSERT INTO PREDICATES VALUES(5,"Loc_Id='HYD'");
        INSERT INTO PREDICATES VALUES(6,"Loc_Id!='HYD'");
        INSERT INTO PREDICATES VALUES(7,"Loc_Id='MUM'");
        INSERT INTO PREDICATES VALUES(8,"Loc_Id!='MUM'");
        
    """)
    conn.commit()
    
    print("inserting into FRAGMENTS table")
    cur.execute("""
        INSERT INTO FRAGMENTS VALUES(1,1,"EMP1",NULL,'HF');
        INSERT INTO FRAGMENTS VALUES(2,1,"EMP2",NULL,'HF');
        INSERT INTO FRAGMENTS VALUES(3,1,"EMP3",NULL,'HF');
        INSERT INTO FRAGMENTS VALUES(4,1,"EMP4",NULL,'HF');
        INSERT INTO FRAGMENTS VALUES(17,2,"DEPARTMENT1",NULL,"NA");
        INSERT INTO FRAGMENTS VALUES(18,3,"LOCATION",NULL,"NA");
        INSERT INTO FRAGMENTS VALUES(5,6,"EMP_DET1",1,'DHF');
        INSERT INTO FRAGMENTS VALUES(6,6,"EMP_DET2",2,'DHF');
        INSERT INTO FRAGMENTS VALUES(7,6,"EMP_DET3",3,'DHF');
        INSERT INTO FRAGMENTS VALUES(8,6,"EMP_DET4",4,'DHF');
        INSERT INTO FRAGMENTS VALUES(9,7,"WORKS1",1,'DHF');
        INSERT INTO FRAGMENTS VALUES(10,7,"WORKS2",2,'DHF');
        INSERT INTO FRAGMENTS VALUES(11,7,"WORKS3",3,'DHF');
        INSERT INTO FRAGMENTS VALUES(12,7,"WORKS4",4,'DHF');
        INSERT INTO FRAGMENTS VALUES(13,4,"DESG",NULL,'NA');
        INSERT INTO FRAGMENTS VALUES(15,5,"PROJECT1",NULL,'VF');
        INSERT INTO FRAGMENTS VALUES(16,5,"PROJECT2",NULL,'VF');
    """)
    conn.commit()
    print("inserting into HF_PREDICATES table")
    cur.execute("""

        INSERT INTO HF_PREDICATES VALUES(1,1); 
        INSERT INTO HF_PREDICATES VALUES(4,1);
        INSERT INTO HF_PREDICATES VALUES(5,1);
        INSERT INTO HF_PREDICATES VALUES(8,1);
        INSERT INTO HF_PREDICATES VALUES(2,2);
        INSERT INTO HF_PREDICATES VALUES(3,2);
        INSERT INTO HF_PREDICATES VALUES(5,2);
        INSERT INTO HF_PREDICATES VALUES(8,2);
        INSERT INTO HF_PREDICATES VALUES(1,3);
        INSERT INTO HF_PREDICATES VALUES(4,3);
        INSERT INTO HF_PREDICATES VALUES(6,3);
        INSERT INTO HF_PREDICATES VALUES(7,3);
        INSERT INTO HF_PREDICATES VALUES(2,4);
        INSERT INTO HF_PREDICATES VALUES(3,4);
        INSERT INTO HF_PREDICATES VALUES(6,4);
        INSERT INTO HF_PREDICATES VALUES(7,4);
        INSERT INTO HF_PREDICATES VALUES(1,5);
        INSERT INTO HF_PREDICATES VALUES(4,5);
        INSERT INTO HF_PREDICATES VALUES(5,5);
        INSERT INTO HF_PREDICATES VALUES(8,5);
        INSERT INTO HF_PREDICATES VALUES(2,6);
        INSERT INTO HF_PREDICATES VALUES(3,6);
        INSERT INTO HF_PREDICATES VALUES(5,6);
        INSERT INTO HF_PREDICATES VALUES(8,6);
        INSERT INTO HF_PREDICATES VALUES(1,7);
        INSERT INTO HF_PREDICATES VALUES(4,7);
        INSERT INTO HF_PREDICATES VALUES(6,7);
        INSERT INTO HF_PREDICATES VALUES(7,7);
        INSERT INTO HF_PREDICATES VALUES(2,8);
        INSERT INTO HF_PREDICATES VALUES(3,8);
        INSERT INTO HF_PREDICATES VALUES(6,8);
        INSERT INTO HF_PREDICATES VALUES(7,8);
        INSERT INTO HF_PREDICATES VALUES(1,9);
        INSERT INTO HF_PREDICATES VALUES(4,9);
        INSERT INTO HF_PREDICATES VALUES(5,9);
        INSERT INTO HF_PREDICATES VALUES(8,9);
        INSERT INTO HF_PREDICATES VALUES(2,10);
        INSERT INTO HF_PREDICATES VALUES(3,10);
        INSERT INTO HF_PREDICATES VALUES(5,10);
        INSERT INTO HF_PREDICATES VALUES(8,10);
        INSERT INTO HF_PREDICATES VALUES(1,11);
        INSERT INTO HF_PREDICATES VALUES(4,11);
        INSERT INTO HF_PREDICATES VALUES(6,11);
        INSERT INTO HF_PREDICATES VALUES(7,11);
        INSERT INTO HF_PREDICATES VALUES(2,12);
        INSERT INTO HF_PREDICATES VALUES(3,12);
        INSERT INTO HF_PREDICATES VALUES(6,12);
        INSERT INTO HF_PREDICATES VALUES(7,12);
        
    """)
    conn.commit()
    print("inserting into VF_COLUMNS table")
    cur.execute("""

        INSERT INTO VF_COLUMNS VALUES
            (17,2,'Dept_Name',false,NULL,NULL);
            
        INSERT INTO VF_COLUMNS VALUES
            (18,3,'Loc_Id',false,NULL,NULL);
        
        INSERT INTO VF_COLUMNS VALUES
            (13,4,'Desgn_Id',false,NULL,NULL),(13,4,'Desgn_Name',false,NULL,NULL),(13,4,'Salary',false,NULL,NULL);
        

        INSERT INTO VF_COLUMNS VALUES
            (1,1,'Emp_Id',false,NULL,NULL),(1,1,'Dept_Name',true,17,'Dept_Name'),(1,1,'Loc_Id',true,18,'Loc_Id'),(1,1,'Desgn_Id',true,13,'Desgn_Id'),(1,1,'Reports_To',true,1,'Emp_Id'),
            (2,1,'Emp_Id',false,NULL,NULL),(2,1,'Dept_Name',true,17,'Dept_Name'),(2,1,'Loc_Id',true,18,'Loc_Id'),(2,1,'Desgn_Id',true,13,'Desgn_Id'),(2,1,'Reports_To',true,2,'Emp_Id'),
            (3,1,'Emp_Id',false,NULL,NULL),(3,1,'Dept_Name',true,17,'Dept_Name'),(3,1,'Loc_Id',true,18,'Loc_Id'),(3,1,'Desgn_Id',true,13,'Desgn_Id'),(3,1,'Reports_To',true,3,'Emp_Id'),
            (4,1,'Emp_Id',false,NULL,NULL),(4,1,'Dept_Name',true,17,'Dept_Name'),(4,1,'Loc_Id',true,18,'Loc_Id'),(4,1,'Desgn_Id',true,13,'Desgn_Id'),(4,1,'Reports_To',true,4,'Emp_Id');
        
        
        

        INSERT INTO VF_COLUMNS VALUES
            (5,6,'Emp_Id',true,1,'Emp_Id'),(5,6,'Age',false,NULL,NULL),(5,6,'Gender',false,NULL,NULL),(5,6,'Name',false,NULL,NULL),(5,6,'Phone_No',false,NULL,NULL),(5,6,'Email_Id',false,NULL,NULL),
            (6,6,'Emp_Id',true,2,'Emp_Id'),(6,6,'Age',false,NULL,NULL),(6,6,'Gender',false,NULL,NULL),(6,6,'Name',false,NULL,NULL),(6,6,'Phone_No',false,NULL,NULL),(6,6,'Email_Id',false,NULL,NULL),
            (7,6,'Emp_Id',true,3,'Emp_Id'),(7,6,'Age',false,NULL,NULL),(7,6,'Gender',false,NULL,NULL),(7,6,'Name',false,NULL,NULL),(7,6,'Phone_No',false,NULL,NULL),(7,6,'Email_Id',false,NULL,NULL),
            (8,6,'Emp_Id',true,4,'Emp_Id'),(8,6,'Age',false,NULL,NULL),(8,6,'Gender',false,NULL,NULL),(8,6,'Name',false,NULL,NULL),(8,6,'Phone_No',false,NULL,NULL),(8,6,'Email_Id',false,NULL,NULL);
        
        INSERT INTO VF_COLUMNS VALUES(15,5,'Project_Id',false,NULL,NULL);
        INSERT INTO VF_COLUMNS VALUES(15,5,'Project_Name',false,NULL,NULL);
        INSERT INTO VF_COLUMNS VALUES(15,5,'Dept_Name',true,17,'Dept_Name');
        
        INSERT INTO VF_COLUMNS VALUES
            (9,7,'Project_Id',true,15,'Project_Id'),(9,7,'Emp_Id',true,1,'Emp_Id'),
            (10,7,'Project_Id',true,15,'Project_Id'),(10,7,'Emp_Id',true,2,'Emp_Id'),
            (11,7,'Project_Id',true,15,'Project_Id'),(11,7,'Emp_Id',true,3,'Emp_Id'),
            (12,7,'Project_Id',true,15,'Project_Id'),(12,7,'Emp_Id',true,4,'Emp_Id');

        
        INSERT INTO VF_COLUMNS VALUES(16,5,'Project_Id',false,NULL,NULL);
        INSERT INTO VF_COLUMNS VALUES(16,5,'Budget',false,NULL,NULL);
        
    """)

    conn.commit()

    cur.execute("""

        INSERT INTO ALLOCATION_MAPPING VALUES(1,'CP5');
        INSERT INTO ALLOCATION_MAPPING VALUES(2,'CP6');
        INSERT INTO ALLOCATION_MAPPING VALUES(3,'CP7');
        INSERT INTO ALLOCATION_MAPPING VALUES(4,'CP8');
        INSERT INTO ALLOCATION_MAPPING VALUES(17,'CP5');
        INSERT INTO ALLOCATION_MAPPING VALUES(18,'CP6');
        INSERT INTO ALLOCATION_MAPPING VALUES(5,'CP5');
        INSERT INTO ALLOCATION_MAPPING VALUES(6,'CP6');
        INSERT INTO ALLOCATION_MAPPING VALUES(7,'CP7');
        INSERT INTO ALLOCATION_MAPPING VALUES(8,'CP8');
        INSERT INTO ALLOCATION_MAPPING VALUES(9,'CP5');
        INSERT INTO ALLOCATION_MAPPING VALUES(10,'CP6');
        INSERT INTO ALLOCATION_MAPPING VALUES(11,'CP7');
        INSERT INTO ALLOCATION_MAPPING VALUES(12,'CP8');
        INSERT INTO ALLOCATION_MAPPING VALUES(13,'CP7');
        INSERT INTO ALLOCATION_MAPPING VALUES(15,'CP5');
        INSERT INTO ALLOCATION_MAPPING VALUES(15,'CP7');
        INSERT INTO ALLOCATION_MAPPING VALUES(16,'CP6');
        INSERT INTO ALLOCATION_MAPPING VALUES(16,'CP8');
        
    """)
    conn.commit()
    

    
if __name__=="__main__":
    ip_address=['10.3.5.211','10.3.5.208','10.3.5.204','10.3.5.205']
    for address in ip_address:
        conn=connect(address)
        cur=conn.cursor()
        create_tables(cur)
        conn.commit()
        insert(cur)
        conn.close()
    
