from logging import root
from threading import local
from turtle import right
from sqlalchemy import true
import System_Catalog as SC
from Utility import Tree

predicate_comparison={'<':{'<':True,'<=':True,'>':'>','>=':'>=','=':'>','!=':True},
'<=':{'<':True,'<=':True,'>':'>','>=':'>=','=':'>=','!=':True},
'>':{'<':'<','<=':'<=','>':True,'>=':True,'=':'<','!=':True},
'>=':{'<':'<','<=':'<=','>':True,'>=':True,'=':'<=','!=':True},
'=':{'<':'<','<=':'<=','>':'>','>=':'>=','=':'==','!=':'!='},
'!=':{'<':True,'<=':True,'>':True,'>=':True,'=':'!=','!=':True}}

def find_leaves(root,leaves):
    if len(root.children)==0:
        leaves.append(root)
    else:
        for child in root.children:
            find_leaves(child,leaves)
def get_root(frag_tree,id):
    id1=int(id)
    while(frag_tree[id1]!=id1):
        id1=frag_tree[id1]
    return id1

def get_fragmentation(node):
    table_name=node.data
    conn=SC.connect("10.3.5.211")
    query="SELECT Frag_Name,Frag_Type,Frag_Id FROM FRAGMENTS,APPLICATION_TABLE where APPLICATION_TABLE.Table_Name='{}' and FRAGMENTS.Table_Id=APPLICATION_TABLE.Table_Id;".format(table_name)
    cursor=conn.cursor()
    cursor.execute(query)
    result=cursor.fetchall()
    conn.close()    
    return result          
def get_primary_key(table_name):
    conn=SC.connect("10.3.5.211")
    query="SELECT Column_Name FROM COLUMNS,APPLICATION_TABLE where APPLICATION_TABLE.Table_Name='{}' and Key_Attribute=true and APPLICATION_TABLE.Table_Id=COLUMNS.Table_Id".format(table_name)
    cursor=conn.cursor()
    cursor.execute(query)
    result=cursor.fetchall()
    conn.close()
    return result

def get_predicates(fragment_id):
    conn=SC.connect("10.3.5.211")
    query="SELECT Predicate_Cond from HF_PREDICATES,PREDICATES where HF_PREDICATES.Frag_Id={} and HF_PREDICATES.Predicate_Id=PREDICATES.Predicate_Id".format(fragment_id)
    cursor=conn.cursor()
    cursor.execute(query)
    result=cursor.fetchall()
    conn.close()
    return result
def process_select_predicate(select_predicate,table_name):
    select_predicate=select_predicate.replace(table_name+".","")
    select_predicate_list=select_predicate.split(" and ")
    return select_predicate_list

def parseCondition(condition):
    columnName=""
    for symbol in ['<','>','!=','<=','>=','=']:
        p=condition.find(symbol)
        if(p!=-1):
            columnName=condition[:p]
            operator=symbol
            value=condition[p+len(symbol):]
            break
    return (columnName,operator,value)

def get_columns_of_fragment(fragment_id):
    conn=SC.connect("10.3.5.211")
    query="SELECT Column_Name from VF_COLUMNS where Frag_Id={}".format(fragment_id)
    cursor=conn.cursor()
    cursor.execute(query)
    result=cursor.fetchall()
    conn.close()
    return result

def build_frag_tree():
    conn=SC.connect("10.3.5.211")
    query="SELECT Frag_Id,Parent_Id from FRAGMENTS"
    cursor=conn.cursor()
    cursor.execute(query)
    result=cursor.fetchall()
    conn.close()
    parent=[None for i in range(len(result)+2)]
    for row in result:
        #print(row)
        if(row[1]==None):
            parent[row[0]]=row[0]
        else:
            parent[row[0]]=row[1]
    return parent

def horizontal_select_reduction(leaf,result):
    print(leaf.data)
    
    select_predicates_list=[]   #find select predicate to be matched with fragment predicate
    if(leaf.parent.data[:6]=="select" or (leaf.parent.parent!=None and leaf.parent.parent.data[:6]=="select")):
        select_predicate_condition=""
        if(leaf.parent.data=="select"):
            select_predicate_condition=leaf.parent.data[7:]
        else:
            select_predicate_condition=leaf.parent.parent.data[7:]
        select_predicates_list=process_select_predicate(select_predicate_condition,leaf.data)
    un=Tree()
    un.data="union"
    for row in result:
        if(len(select_predicates_list)==0):
            
            temp=Tree()
            temp.data=str(row[2])+" "+row[0]+" "+row[1]+" "+leaf.data
            temp.parent=un
            un.children.append(temp)
        else:
            cur_frag_predicates=get_predicates(row[2])
            cur_predicates=[x[0] for x in cur_frag_predicates]
            frag_satisfy=True
            for complex_predicate in select_predicates_list:
                complex_predicate_satisfy=False
                
                if frag_satisfy==True:
                    simple_predicates_list=complex_predicate.split(" or ")
                    for simple_predicate in simple_predicates_list:
                        simple_predicate_satisfy=True
                        col1,op1,val1=parseCondition(simple_predicate)
                        for predicate in cur_predicates:
                            col2,op2,val2=parseCondition(predicate)
                            if(col1==col2):
                                op=predicate_comparison[op1][op2]
                                if(op==True):
                                    check="True"
                                else:
                                    check="{}{}{}".format(val1,op,val2)
                                simple_predicate_satisfy=simple_predicate_satisfy and eval(check)
                            else:
                                simple_predicate_satisfy=simple_predicate_satisfy and True
                        complex_predicate_satisfy=complex_predicate_satisfy or simple_predicate_satisfy
                    frag_satisfy=frag_satisfy and complex_predicate_satisfy
                else:
                    break
            if(frag_satisfy==True): 
                temp=Tree()
                temp.data=str(row[2])+" "+row[0]+" "+row[1]+" "+leaf.data
                temp.parent=un
                un.children.append(temp)
    
    if(len(un.children)==1):
        leaf.parent.children.remove(leaf)
        un.children[0].parent=leaf.parent
        leaf.parent.children.append(un.children[0])
        del un
        del leaf
    else:
        
        project_node=leaf.parent
        select_node=None
        if(project_node.parent==None):
            idx=project_node.children.index(leaf)
            project_node.children.remove(leaf)
            project_node.children.append(un)
        elif(project_node.parent.data[:6]=="select"):
            select_node=project_node.parent
            if(select_node==None):
                print("hi1")
                un.parent=project_node.parent
                un.parent.children.append(un)
                
                project_node.parent.children.remove(project_node)
                
                un_childs=un.children.copy()
                un.children.clear()
                project_node.children.remove(leaf) 
                del leaf
                for child in un_childs:
                    local_project=Tree()
                    local_project.data=project_node.data
                    child.parent=local_project
                    local_project.children.append(child)
                    local_project.parent=un
                    un.children.append(local_project)
                del project_node
                
            else:
                un.parent=select_node.parent
                id=select_node.parent.children.index(select_node)
                select_node.parent.children[id]=un
                un_childs=un.children.copy()
                un.children.clear()
                project_node.children.remove(leaf)
                del leaf
                for child in un_childs:
                    local_select=Tree()
                    local_select.data=select_node.data
                    local_project=Tree()
                    local_project.data=project_node.data
                    local_select.parent=un
                    un.children.append(local_select)
                    local_project.parent=local_select
                    local_select.children.append(local_project)
                    child.parent=local_project
                    local_project.children.append(child)
        else:
            un.parent=project_node.parent
            idx=un.parent.children.index(project_node)
            

            un.parent.children[idx]=un
            childs=un.children.copy()
            un.children.clear()
            for child in childs:
                local_project=Tree()
                local_project.data=project_node.data
                child.parent=local_project
                local_project.children.append(child)
                un.children.append(local_project)



            
def vertical_reduction(leaf,result):
    pks=get_primary_key(leaf.data)
    join_node=Tree()
    join_attribute=[row[0] for row in pks] 
    join_node.data="vf join "+",".join(join_attribute)
    final_attributes=leaf.parent.data[8:]
    final_attributes=final_attributes.split(",")
    print("final attributes",final_attributes)
    for row in result:
        temp=Tree()
        temp.data=str(row[2])+" "+row[0]+" "+"VF"+" "+leaf.data
        if(final_attributes[0]!='*'):
            columns=get_columns_of_fragment(row[2])
            column_names=[x[0] for x in columns]
            #print("column names",column_names)
            local_columns=set(join_attribute)
            for column in column_names:
                if(column in final_attributes):
                    local_columns.add(column)
            #print("local_columns",local_columns)
            #print("join_attribute",join_attribute)
            if(len(local_columns)>len(join_attribute)):
                temp_project=Tree()
                temp_project.data="project "+",".join(local_columns)
                temp_project.parent=join_node
                join_node.children.append(temp_project)
                temp_project.children.append(temp)
                temp.parent=temp_project
        else:
            temp_project=Tree()
            temp_project.data="project *"
            temp_project.parent=join_node
            join_node.children.append(temp_project)
            temp_project.children.append(temp)
            temp.parent=temp_project
    if(len(join_node.children)==1):
        join_node.children[0].children[0].parent=leaf.parent
        leaf.parent.children.remove(leaf)
        leaf.parent.children.append(join_node.children[0].children[0])
        del leaf
        del join_node.children[0]
        del join_node
    else:
        print("test",len(join_node.children))
        if(leaf.parent.parent==None):
            join_node.parent=leaf.parent
            idx=leaf.parent.children.index(leaf)
            leaf.parent.children[idx]=join_node
            del leaf    
        else:
            join_node.parent=leaf.parent.parent
            idx=leaf.parent.parent.children.index(leaf.parent)
            leaf.parent.parent.children[idx]=join_node
            leaf.parent.children.remove(leaf)        
        

def get_fragments_of_subtree(node,fragments):
    if(len(node.children)==0 and node.data[:7]!="project"):
        fragments.append(node.data)
    else:
        for child in node.children:
            get_fragments_of_subtree(child,fragments)

def find_join(root):
    if(root.data[:4].lower()=="join"):
        return root
    for child in root.children:
        temp=find_join(child)
        if(temp!=None):
            return temp
    return None
def duplicate(subtree_root):
    
    if(len(subtree_root.children)==0):
        temp=Tree()
        temp.data=subtree_root.data
        temp.children=subtree_root.children
        return temp
    else:
        temp=Tree()
        temp.data=subtree_root.data
        for child in subtree_root.children:
            res=duplicate(child)
            res.parent=temp
            temp.children.append(res)
        return temp

def join_distribution(node):
    if(node.children[0].data[:4]=="join"):
        join_distribution(node.children[0])
    if(node.children[1].data[:4]=="join"):
        join_distribution(node.children[1])
    if(node.children[0].data=="union" and node.children[1].data=="union"):
        un=Tree()
        un.data="union"
        un.parent=node.parent
        idx=node.parent.children.index(node)
        node.parent.children[idx]=un
        for child1 in node.children[0].children:
            for child2 in node.children[1].children:
                jn=Tree()
                jn.data=node.data
                dup1=duplicate(child1)
                dup2=duplicate(child2)
                jn.children.append(dup1)
                jn.children.append(dup2)
                dup1.parent=jn
                dup2.parent=jn
                jn.parent=un
                un.children.append(jn)
        return un
        

    elif(node.children[0].data=="union"):
        un=Tree()
        un.data="union"
        un.parent=node.parent
        idx=node.parent.children.index(node)
        node.parent.children[idx]=un
        for child1 in node.children[0].children:
            jn=Tree()
            jn.data=node.data
            jn.parent=un
            dup1=duplicate(child1)
            dup2=duplicate(node.children[1])
            jn.children.append(dup1)
            jn.children.append(dup2)
            dup1.parent=jn
            dup2.parent=jn
            un.children.append(jn)
        return un
    
    elif(node.children[1].data=="union"):
        print("hi----")
        un=Tree()
        un.data="union"
        un.parent=node.parent
        idx=node.parent.children.index(node)
        node.parent.children[idx]=un
        for child2 in node.children[1].children:
            jn=Tree()
            jn.data=node.data
            jn.parent=un
            dup1=duplicate(node.children[0])
            dup2=duplicate(child2)
            jn.children.append(dup1)
            jn.children.append(dup2)
            dup1.parent=jn
            dup2.parent=jn
            un.children.append(jn)
        return un
    else:
        pass
            

def get_table_id(frag_id):
    conn=SC.connect("10.3.5.211")
    query="SELECT Table_Id from FRAGMENTS where Frag_Id={}".format(frag_id)
    cursor=conn.cursor()
    cursor.execute(query)
    result=cursor.fetchall()
    conn.close()
    return result[0][0]



def join_reduction(join_node,frag_tree):
    left=[]
    right=[]
    x=True
    y=True
    
    if(join_node.children[0].data[:4]=="join"):
        x=join_reduction(join_node.children[0],frag_tree)    
    if(join_node.children[1].data[:4]=="join"):
        y=join_reduction(join_node.children[1],frag_tree)
    if(x==True and y==True):
        get_fragments_of_subtree(join_node.children[0],left)
        get_fragments_of_subtree(join_node.children[1],right)
        for frag1 in left:
            frag_id1,frag_name1,frag_type1,table_name1=frag1.split(" ")
            root1=get_root(frag_tree,frag_id1)
            x1=get_table_id(root1)
            for frag2 in right:
                print(right)
                frag_id2,frag_name2,frag_type2,table_name2=frag2.split(" ")
                if(join_node.data.find(table_name1)!=-1 and join_node.data.find(table_name2)!=-1):
                    if(frag_type1=="DHF" or frag_type2=="DHF"):
                        root2=get_root(frag_tree,frag_id2)
                        
                        x2=get_table_id(root2)
                        
                        if(x1!=x2):
                            return True
                        else:
                            if(root1==root2):
                                return True
                            else:
                                return False

                        
                    elif(frag_type1=="VF" or frag_type2=="VF"):
                        return True
                    
                    elif(frag_type1=="HF" and frag_type2=="HF"):
                        result1=get_predicates(frag_id1)
                        result2=get_predicates(frag_id2)
                        join_predicate=join_node.data[5:]
                        table1_column,table2_column=join_predicate.split("=")
                        b1=False
                        b2=False
                        b1_predicate=None
                        b2_predicate=None
                        if(table1_column[:len(table_name1)+1]==table_name1+"."):
                            column_name=table1_column[len(table_name1)+2:]
                            for row in result1:
                                col,op,val=parseCondition(row[0])
                                b1=b1 or (col==column_name)
                                if(b1==True):
                                    b1_predicate=row[0]
                                    break
                            column_name=table2_column[len(table_name2)+2:]
                            for row in result2:
                                col,op,val=parseCondition(row[0])
                                b2=b2 or (column_name==col)
                                if(b2==True):
                                    b2_predicate=row[0]
                                    break
                        else:
                            column_name=table1_column[len(table_name2)+2:]
                            for row in result1:
                                col,op,val=parseCondition(row[0])
                                b1=b1 or (col==column_name)
                                if(b1==True):
                                    b1_predicate=row[0]
                                    break
                            column_name=table2_column[len(table_name1)+2:]
                            for row in result2:
                                col,op,val=parseCondition(row[0])
                                b2=b2 or (col==column_name)
                                if(b2==True):
                                    b2_predicate=row[0]
                                    break
                        if(b1 and b2):
                            col1,op1,val1=parseCondition(b1_predicate)
                            col2,op2,val2=parseCondition(b2_predicate)
                            op=predicate_comparison[op1][op2]
                            if(op==True):
                                return True
                            else:
                                check="{}{}{}".format(val1,op,val2)
                                return eval(check)
                        else:
                            return False
                    
    elif(x==False and y==True):
       del join_node.children[0]
       return False 
    elif(x==True and y==False):
        del join_node.children[1]
        return False
    else:
        join_node.children.clear()
        return False        

def localize(optimized_tree):
    leaves=[]
    frag_tree=build_frag_tree()
    
    find_leaves(optimized_tree,leaves)
    for leaf in leaves:
        
        result=get_fragmentation(leaf)
        if(result[0][1]=='HF' or result[0][1]=='DHF'):  #if a table is fragmented into horizontal fragment
            horizontal_select_reduction(leaf,result)

        elif(result[0][1]=='VF'):
            vertical_reduction(leaf,result)
        
        else:
            new_leaf=Tree()
            new_leaf.data=str(result[0][2])+" "+result[0][0]+" "+result[0][1]+" "+leaf.data
            new_leaf.parent=leaf.parent
            idx=leaf.parent.children.index(leaf)
            leaf.parent.children[idx]=new_leaf

    
    
    
    first_join=find_join(optimized_tree)
    if(first_join!=None):
        un=join_distribution(first_join)
        childs=un.children.copy()
        for child in childs:
            res=join_reduction(child,frag_tree)
            if(res==False):
                un.children.remove(child)
    