from Utility import *
import sqlparse
from QueryParser import parse_query

from DataStructure import query_to_alias

def decompose_query(clause_dict, condition_concat, attribute_table_map):

    join_query = []
    direct_query = []
    direct_query_nodes = []
    join_query_nodes = []
    direct_query = {}
    # condition_concat = None

    select_clause = clause_dict['select']
    from_clause = clause_dict['from']
    where_clause = clause_dict['where']
    group_by_clause = clause_dict['group by']
    having_clause = clause_dict['having']

    print('select_clause', select_clause)
    print('from_clause', from_clause)
    print('where_clause', where_clause)
    print('group_by', group_by_clause)
    print('having', having_clause)

    for condition in where_clause:

        if(len(where_clause) == 0):
            return None
        print('condition ---', condition)
        left_part, comparison, right_part = break_query(condition)

        if(isinstance(right_part, sqlparse.sql.Parenthesis)):
            sel_clause, fr_clause, whr_clause, gp_by_clause = parse_query(right_part)
            subtree = decompose_query(sel_clause, fr_clause, whr_clause, gp_by_clause, attribute_table_map)
            direct_query_nodes.append(subtree)
        
        
        elif(isinstance(condition[0], sqlparse.sql.Identifier) and isinstance(condition[2], sqlparse.sql.Identifier)):
            continue
        
        else:
            table_name = get_table_name(left_part.value, from_clause, attribute_table_map)
            if(table_name not in direct_query):
                direct_query[table_name] = []
            direct_query[table_name].append(condition.value)
    
    # print('--------- and -----------')
    # for condn in condition_concat['and']:
    #     print(condn.value)
    # print('--------- or -----------')
    # for condn in condition_concat['or']:
    #     print(condn.value)

    for table_name, queries in direct_query.items():
        query = None
        for idx, q in enumerate(queries):
            if(idx == 0):
                query = q
                prev = q
            elif(find_concat_keyword(condition_concat['and'], q, prev)):
                query = query + ' and ' + q
                prev = q
            else:
                query = query + ' or ' + q
                prev = q
            
            
        direct_query_node = build_tree_from_direct_query(table_name, query)
        direct_query_nodes.append(direct_query_node)
    
    for condition in where_clause:

        if(len(where_clause) == 0):
            return None

        if(isinstance(condition[0], sqlparse.sql.Identifier) and isinstance(condition[2], sqlparse.sql.Identifier)):
            join_query.append(condition)

            lhs_table = get_table_name(condition[0].value, from_clause, attribute_table_map)
            rhs_table = get_table_name(condition[2].value, from_clause, attribute_table_map)

            left_node = None
            right_node = None
            

            # scan the direct_query_nodes list to find out the correct pair of nodes to be joined
            for idx, query in enumerate(direct_query_nodes):
                print('query ======', query)
                if(query == None):
                    continue
                q_l, q_r = split_query(query.data.split(' ',1)[1])
                table_name = get_table_name(q_l.strip(), from_clause, attribute_table_map)
                if(table_name == lhs_table):
                    left_node = query
                    direct_query_nodes[idx] = None
                elif(table_name == rhs_table):
                    right_node = query
                    direct_query_nodes[idx] = None
                    
            if(left_node == None):
                left_node = get_child_node(lhs_table, attribute_table_map)
            if(right_node == None):
                right_node = get_child_node(rhs_table, attribute_table_map)
            join_query_node = build_tree_from_join_query(condition.value, [left_node, right_node])
            join_query_nodes.append(join_query_node)
    
    join_nodes = []
    and_node = None
    for idx, join_query_node in enumerate(join_query_nodes):
        if(idx == 0):
            query = join_query_node
            prev = join_query_node
            root = join_query_node
            cur_node = root
        elif(find_concat_keyword(condition_concat['and'], join_query_node.data.split('join ',1)[1], prev.data.split('join ',1)[1])):
            cur_node_children = cur_node.children
            cur_node.children = [join_query_node]

            all_children = []
            for child in join_query_node.children:
                all_children.append(child.children[0].data)
            
            for child in cur_node_children:
                print('child:', child.data)
                if(child.children[0].data not in all_children):
                    cur_node.children.append(child)
                elif(child.data.startswith('select')):
                    for idx, c in enumerate(join_query_node.children):
                        if(join_query_node.children[idx].children[0].data == child.children[0].data):
                            join_query_node.children[idx] = child

            join_query_node.parent = cur_node
            cur_node = join_query_node
            and_node = root
            prev = join_query_node
        else:
            query = query_to_alias[join_query_node.data] + ' UNION ' + query_to_alias[prev.data]
            join_nodes.append(build_tree_from_join_query(query, [join_query_node, prev], ''))
            prev = join_query_node

    if(and_node!=None):
        join_nodes.append(and_node)    
        
    for query in direct_query_nodes:
        lst = []
        if(query != None):
            lst.append(query)
        direct_query_nodes = lst

    if(len(where_clause) == 0):
        children_list = []
        for table in from_clause:
            child_node = Tree()
            child_node.data = table
            children_list.append(child_node)
        query = ','.join(table for table in from_clause)
        if(len(from_clause)>1):
            join_query_nodes.append(build_tree_from_join_query(query, children_list, clause='cartesian product '))
        # elif(len(from_clause)==1):
        #     select_clause.append(from_clause[0])
            # direct_query_nodes.append(build_tree_from_direct_query(from_clause[0], '*'))
    
    if(len(join_nodes)>0):
        join_query_nodes = join_nodes
    
    root = add_root(select_clause, join_query_nodes, direct_query_nodes, group_by_clause, having_clause)
    return root 
