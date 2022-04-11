

def sdd(input,ping_cost,profiles,all_selectivities):
    output=[]
    for join,selectivity_per_join in zip(input,all_selectivities):
        profits=[]
        max_profit=-1
        for label,(semi_join,selectivity )in enumerate(zip(join,selectivity_per_join)):
            total_size=0
            for col in profiles[semi_join[1]]['Cols']:
                total_size+=profiles[semi_join[1]][col]['size']
            benefit=(1-selectivity)*(total_size)*(profiles[semi_join[1]]['card'])
            cost=ping_cost[semi_join[2]][semi_join[0]]*profiles[semi_join[3]][semi_join[5]]['val']
            profit=benefit-cost
            profits.append(profit)
            max_profit=max(max_profit,profit)

        id=profits.index(max_profit)
        
        
