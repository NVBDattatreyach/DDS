
import System_Catalog as SC
from Utility import Tree

def post_order(node,table_to_view,view_to_frag,local_queries,temp_value,graph):
    if(len(node.children)==0):
        
        frag_id,frag_name,frag_type,table_name=node.data.split(" ")
        project_node=node.parent
        view_name="v{}".format(temp_value[0])
        query="""create Table {} as select """.format(view_name)
        temp_value[0]+=1
        query=query+project_node.data[8:]
        query=query+""" from """
        query=query+frag_name+""" """
        select_node=None
        if(project_node.parent.data[:6]=="select"):
            select_node=project_node.parent
            select_node.data=select_node.data.replace(table_name+".",frag_name+".")
            query=query+"""where """+select_node.data[7:]
        query=query.strip(" ")
        local_queries[view_name]=(frag_id,frag_name,query,project_node.data[8:])
        if(table_name in table_to_view):
            table_to_view[table_name].append(view_name)
        else:
            table_to_view[table_name]=[view_name]
        view_to_frag[view_name]=frag_id
        temp=Tree()
        temp.data=view_name
        graph[view_name]=[]
        if(select_node==None):
            idx=project_node.parent.children.index(project_node)
            project_node.parent.children[idx]=temp
            temp.parent=project_node.parent
            
        else:
            idx=select_node.parent.children.index(select_node)
            select_node.parent.children[idx]=temp
            temp.parent=select_node.parent
            
        
    else:
        for child in node.children:
            post_order(child,table_to_view,view_to_frag,local_queries,temp_value,graph)
            if(node.data=="union"):
                table_to_view.clear()
        if(node.data[:4]=="join"):
            join_condition=node.data[5:]
            a,b=join_condition.split("=")
            t1,c1=a.split(".")
            t2,c2=b.split(".")
            new_joins=[]
            for v1 in table_to_view[t1]:
                for v2 in table_to_view[t2]:
                    new_joins.append("{}.{}={}.{}".format(v1,c1,v2,c2))
                    graph[v1].append((v2,c1))
                    graph[v2].append((v1,c2))
            join_condition=" and ".join(new_joins)
            node.data="join "+join_condition
            
        if(node.data[:7]=="vf join"):
            join_condition=node.data[8:]
            all_joins=[]
            children=[child.data for child in node.children]
            for  i in range(len(children)):
                for j in range(i+1,len(children)):
                    all_joins.append("{}.{}={}.{}".format(children[i],join_condition,children[j],join_condition))
                    graph[children[i]].append((children[j],join_condition))
                    graph[children[j]].append((children[i],join_condition))
            final_join_str="join "+" and ".join(all_joins)
            node.data=final_join_str


def update_tree(root,temp_value):
    table_to_frag={}
    view_to_frag={}
    local_queries={}
    graph={}
    post_order(root,table_to_frag,view_to_frag,local_queries,temp_value,graph)
    return (local_queries,view_to_frag,graph)

def preorder(root,sdd_input,view_to_frag):
    if(root.data[:4]=="join"):
        join_conditions=root.data[5:]
        l=join_conditions.split(" and ")
        for condition in l:
            a,b=condition.split("=")
            v1,col1=a.split(".")
            v2,col2=b.split(".")
            sdd_input.append([view_to_frag[v1],v1,view_to_frag[v2],v2,col1,col2])
            sdd_input.append([view_to_frag[v2],v2,view_to_frag[v1],v1,col2,col1])
    
    for child in root.children:
        preorder(child,sdd_input,view_to_frag)

def find_union(root):
    
    if(root.data[:5]=="union"):
        return root
    for child in root.children:
        x=find_union(child)
        if(x!=None):
            return x
    return None
    
def find_join(root):
    if(root.data[:4]=="join"):
        return root
    for child in root.children:
        x=find_join(child)
        if(x!=None):
            return x
    return None
def create_sdd_input(root,view_to_frag):
    sdd_input=[]
    union_node=find_union(root)
    if(union_node!=None):
        for children in union_node.children:
            if children.data[:4]=="join":
                sub_tree_sdd_input=[]
                preorder(children,sub_tree_sdd_input,view_to_frag)
                sdd_input.append(sub_tree_sdd_input)
            
        return sdd_input
    else:
        join_node=find_join(root)
        if(join_node!=None):
            sub_tree_sdd_input=[]
            preorder(join_node,sub_tree_sdd_input,view_to_frag)
            sdd_input.append(sub_tree_sdd_input)
            return sdd_input
        else:
            return []


def update_sdd_input(sdd_input):
    view_to_site={}
    conn=SC.connect("10.3.5.211")
    for row in sdd_input:
        for row1 in row:
            frag_id1=row1[0]
            frag_id2=row1[2]
            
            query="SELECT SITE_INFO.Host_Name from ALLOCATION_MAPPING,SITE_INFO where ALLOCATION_MAPPING.Host_Name=SITE_INFO.Host_Name and ALLOCATION_MAPPING.Frag_Id={} ORDER BY SITE_INFO.Host_Name".format(frag_id1)
            cur=conn.cursor()
            cur.execute(query)
            result=cur.fetchall()
            view_to_site[frag_id1]=result[0][0]
            row1[0]=view_to_site[frag_id1]
            query="SELECT SITE_INFO.Host_Name from ALLOCATION_MAPPING,SITE_INFO where ALLOCATION_MAPPING.Host_Name=SITE_INFO.Host_Name and ALLOCATION_MAPPING.Frag_Id={} ORDER BY SITE_INFO.Host_Name".format(frag_id2)
            cur=conn.cursor()
            cur.execute(query)
            result=cur.fetchall()
            view_to_site[frag_id2]=result[0][0]
            row1[2]=view_to_site[frag_id2]     
    return sdd_input


