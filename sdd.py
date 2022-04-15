

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
    profiles[new_name]=profiles[old_name].copy()
    profiles[new_name]['card']=selectivity*profiles[old_name]['card']
    profiles[new_name][join_attribute]['val']=selectivity*profiles[old_name][join_attribute]['val']
    for attr in profiles[new_name]['Cols']:
        if attr!=join_attribute:
            profiles[new_name][join_attribute]['val']=c(profiles[old_name]['card'],profiles[old_name][join_attribute]['val'],profiles[new_name]['card'])

def update_join(join,old_name,new_name):
    for row in join:
        if(row[1]==old_name):
            row[1]=new_name
        elif(row[3]==old_name):
            row[3]=new_name
def calculate_selectivity(join,profiles):
    selectivities=[]
    for row in join:
        selectivity=min(1,profiles[row[3]][row[5]]['val']/ profiles[row[3]][row[5]]['dom'])
        selectivities.append(selectivity)
    return selectivities

def sdd1(join,ping_cost,profiles,selectivities,semi_join_order,final_reductions):
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
        old_name=join[max_id][1]
        join_attribute=join[max_id][4]
        new_name=old_name+"'"
        p=new_name.find("'")
        final_reductions[new_name[:p]][0]=new_name
        output.append(new_name)
        semi_join_order.append(output)
        print(old_name,"->",new_name)
        join.pop(max_id)
        # calculate new profile for the reduced table
        update_profile(profiles,old_name,new_name,final_selectivity,join_attribute)
        # update input table,update selectivities and calculate sdd
        update_join(join,old_name,new_name)
        new_selectivities=calculate_selectivity(join,profiles)
        sdd1(join,ping_cost,profiles,new_selectivities,semi_join_order,final_reductions)
    else:
        semi_join_order.extend(join)
    
        
        
