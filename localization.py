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
def get_fragmentation(node):
    table_name=node.data
    conn=SC.connect("10.3.5.211")
    query="SELECT Frag_Name,Frag_Type,Frag_Id FROM FRAGMENTS,APPLICATION_TABLE where APPLICATION_TABLE.Table_Name='{}' and FRAGMENTS.Table_Id=APPLICATION_TABLE.Table_Id;".format(table_name)
    print(query)
    cursor=conn.cursor()
    cursor.execute(query)
    result=cursor.fetchall()
    conn.close()    
    return result          
def get_primary_key(table_name):
    conn=SC.connect("10.3.5.211")
    query="SELECT Column_Name FROM COLUMNS,APPLICATION_TABLE where APPLICATION_TABLE.Table_Name='{}' and Key_Attribute=true and APPLICATION_TABLE.Table_Id=COLUMNS.Table_Id".format(table_name)
    print(query)
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

def horizontal_select_reduction(leaf,result):
    select_predicates_list=[]   #find select predicate to be matched with fragment predicate
    if(leaf.parent.data[:6]=="select" or (leaf.parent.parent!=None and leaf.parent.parent.data[:6]=="select")):
        select_predicate_condition=""
        if(leaf.parent.data=="select"):
            select_predicate_condition=leaf.parent.data[7:]
        else:
            select_predicate_condition=leaf.parent.parent.data[7:]
        select_predicates_list=process_select_predicate(select_predicate_condition,leaf.data)
    un=Tree()
    un.data="UNION"
    for row in result:
        if(len(select_predicates_list)==0):
            temp=Tree()
            temp.data=str(row[2])+" "+row[0]
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
                temp.data=str(row[2])+" "+row[0]
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
        if(project_node.parent.data[:6]=="select"):
            select_node=project_node.parent
        if(select_node==None):
            #projects parent as unions parent
            un.parent=project_node.parent
            un.parent.children.append(un)
            #unlink project from its parent
            project_node.parent.children.remove(project_node)
            #make union as projects parent
            project_node.parent=un
            un_childs=un.children.copy()
            un.children.clear()
            #unions child is only project node
            un.children.append(project_node)
            #remove actual table from query tree
            project_node.children.remove(leaf) 
            for child in un_childs:
                child.parent=project_node
                project_node.children.append(child)
            
            
            
        else:
            un.parent=select_node.parent
            select_node.parent.children.remove(select_node)
            select_node.parent.children.append(un)
            un_childs=un.children.copy()
            un.children.clear()
            un.children.append(select_node)
            select_node.parent=un
            project_node.children.remove(leaf)
            for child in un_childs:
                child.parent=project_node
                project_node.children.append(child)
            
def vertical_reduction(leaf,result):
    pks=get_primary_key(leaf.data)
    join_node=Tree()
    join_attribute=[row[0] for row in pks] 
    join_node.data="join "+",".join(join_attribute)
    final_attributes=leaf.parent.data[8:]
    final_attributes=final_attributes.split(",")
    for row in result:
        temp=Tree()
        temp.data=str(row[2])+" "+row[0]
        if(final_attributes[0]!='*'):
            columns=get_columns_of_fragment(row[2])
            column_names=[x[0] for x in columns]
            print("column names",column_names)
            local_columns=set(join_attribute)
            for column in column_names:
                if(column in final_attributes):
                    local_columns.add(column)
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
        join_node.parent=leaf.parent
        leaf.parent.children.remove(leaf)
        join_node.parent.children.append(join_node)

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
    return root

def join_distribution(node):

    left_frags=[]
    get_fragments_of_subtree(node.children[0],left_frags)
    right_frags=[]
    get_fragments_of_subtree(node.children[1],right_frags)

    if(node.children[0].data.lower()=="union" or node.children[1].data.lower()=="union"):
        final_joins=[]
        for l_frag in left_frags:
            l_frag_id,l_frag_name=l_frag.split(" ")
            
            for r_frag in right_frags:
                r_frag_id,r_frag_name=r_frag.split(" ")
                


def localize(optimized_tree):
    leaves=[]
    find_leaves(optimized_tree,leaves)
    for leaf in leaves:
        
        result=get_fragmentation(leaf)
        print(result)
        if(result[0][1]=='HF' or result[0][1]=='DHF'):  #if a table is fragmented into horizontal fragment
            horizontal_select_reduction(leaf,result)

        elif(result[0][1]=='VF'):
            vertical_reduction(leaf,result)

        """
        first_join=find_join(optimized_tree)
        join_distribution(first_join)
        """
