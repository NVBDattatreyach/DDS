import copy

def sdd(input,ping_cost,profiles,all_selectivities):
    output=[]
    for join,selectivity_per_join in zip(input,all_selectivities):
        profits=[]
        max_profit=-1
        for semi_join,selectivity in zip(join,selectivity_per_join):
            total_size=0
            for col in profiles[semi_join[1]]['Cols']:
                total_size+=profiles[semi_join[1]][col]['size']
            benefit=(1-selectivity)*(total_size)*(profiles[semi_join[1]]['card'])
            cost=ping_cost[semi_join[2]][semi_join[0]]*profiles[semi_join[3]][semi_join[5]]['val']
            profit=benefit-cost
            profits.append(profit)
            max_profit=max(max_profit,profit)

        id=profits.index(max_profit)
        reduced_table_name=join[id][1]
        temp=temp+1
        new_table_name="v"+str(temp)

def c(n,m,r):
    if(r<m/2):
        return r
    elif(m/2<=r and r<2*m):
        return (r+m)/3
    else:
        return m
def update_profile(profiles,old_name,new_name,selectivity,join_attribute):
    profiles[new_name]=copy.deepcopy(profiles[old_name])
    profiles[new_name]['card']=selectivity*profiles[old_name]['card']
    profiles[new_name][join_attribute]['val']=selectivity*profiles[old_name][join_attribute]['val']
    
    # print("test",new_name,old_name,selectivity,profiles[new_name][join_attribute]['val'],profiles[old_name][join_attribute]['val'])
    for attr in profiles[new_name]['Cols']:
        if attr!=join_attribute:
            profiles[new_name][attr]['val']=c(profiles[old_name]['card'],profiles[old_name][attr]['val'],profiles[new_name]['card'])

def update_join(join,old_name,new_name):
    for row in join:
        if(row[1]==old_name):
            row[1]=new_name
        elif(row[3]==old_name):
            row[3]=new_name


def search(cur_node,target_node,graph,visited):
    
    visited[cur_node]=True
    if(cur_node==target_node):
        return True
    if(cur_node not in graph):
        return False
    for child in graph[cur_node]:
        if(child not in visited):
            x=search(child,target_node,graph,visited)
            if(x==True):
                return True
    return False
        

def calculate_selectivity(join,profiles,semi_join_graph):
    selectivities=[]
    # for k,v in semi_join_graph.items():
    #     print(k,"->",v)
    for row in join:
        selectivity=0
        visited={}
        x=search(row[3],row[1],semi_join_graph,visited)
        if(x==False):
            selectivity=min(1,profiles[row[3]][row[5]]['val']/ profiles[row[3]][row[5]]['dom'])
        else:
            selectivity=min(1,profiles[row[3]][row[5]]['val']/ profiles[row[1]][row[4]]['val'])
        selectivities.append(selectivity)
        # print(row,x,selectivity)
    return selectivities

def sdd1(join,ping_cost,profiles,selectivities,semi_join_order,final_reductions,semi_join_graph):
    max_profit=0
    max_id=0
    final_selectivity=0
    # calculate best semi_join and store it in output
    for id,(semi_join,selectivity) in enumerate(zip(join,selectivities)):
        total_size=0
        for col in profiles[semi_join[1]]['Cols']:
            total_size+=profiles[semi_join[1]][col]['size']
        benefit=(1-selectivity)*(total_size)*(profiles[semi_join[1]]['card'])
        cost=ping_cost[semi_join[2]][semi_join[0]]*profiles[semi_join[3]][semi_join[5]]['val']
        profit=benefit-cost
        if(profit>max_profit):
           max_id=id
           max_profit=profit
           final_selectivity=selectivity
    if(max_profit>0):
        output=join[max_id]
        #print("selected",output)
        old_name=join[max_id][1]
        join_attribute=join[max_id][4]
        new_name=old_name+"_"
        p=new_name.find("_")
        final_reductions[new_name[:p]][0]=new_name
        output.append(new_name)
        semi_join_order.append(output)
        #print(old_name,"->",new_name)
        semi_join_graph[new_name]=[join[max_id][3]]
        if(join[max_id][3] in semi_join_graph):
            semi_join_graph[join[max_id][3]].append(new_name)
        else:
            semi_join_graph[join[max_id][3]]=[new_name]
        join.pop(max_id)
        # calculate new profile for the reduced table
        update_profile(profiles,old_name,new_name,final_selectivity,join_attribute)
        # update input table,update selectivities and calculate sdd
        update_join(join,old_name,new_name)
        new_selectivities=calculate_selectivity(join,profiles,semi_join_graph)
        sdd1(join,ping_cost,profiles,new_selectivities,semi_join_order,final_reductions,semi_join_graph)
    
    
        
        
