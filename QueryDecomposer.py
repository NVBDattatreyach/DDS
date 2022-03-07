from Utility import *
import sqlparse
from QueryParser import parse_query



def decompose_query(select_clause, from_clause, where_clause, group_by_clause, attribute_table_map):

    join_query = []
    direct_query = []
    direct_query_nodes = []
    join_query_nodes = []
    direct_query = {}
    condition_concat = None

    print('select_clause', select_clause)
    print('from_clause', from_clause)
    print('where_clause', where_clause)

    for condition in where_clause:

        if(len(where_clause) == 0):
            return None
        
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
    
    for table_name, queries in direct_query.items():
        queries = ','.join(queries)
        direct_query_node = build_tree_from_direct_query(table_name, queries)
        direct_query_nodes.append(direct_query_node)
    
    for condition in where_clause:

        if(len(where_clause) == 0):
            return None

        if(isinstance(condition[0], sqlparse.sql.Identifier) and isinstance(condition[2], sqlparse.sql.Identifier)):
            join_query.append(condition)
            # lhs_table = ((condition[0].value).split('.'))[0]
            # rhs_table = ((condition[2].value).split('.'))[0]

            lhs_table = get_table_name(condition[0].value, from_clause, attribute_table_map)
            rhs_table = get_table_name(condition[2].value, from_clause, attribute_table_map)

            left_node = None
            right_node = None
            

            # scan the direct_query_nodes list to find out the correct pair of nodes to be joined
            # also assuming that only one attribute is used to join 2 tables
            for idx, query in enumerate(direct_query_nodes):
                # table_name = (((query.data.split())[1]).split('.'))[0]
                q_l, q_r = split_query(query.data.split(' ',1)[1])
                table_name = get_table_name(q_l.strip(), from_clause, attribute_table_map)
                if(table_name == lhs_table):
                    left_node = query
                    direct_query_nodes[idx] = None
                    # direct_query_nodes.pop(idx)
                elif(table_name == rhs_table):
                    right_node = query
                    direct_query_nodes[idx] = None
                    # direct_query_nodes.pop(idx)
                # print('len_direct_q:', len(direct_query_nodes))
            # if(left_node!=None and right_node!=None):
            #     print('left_child:', left_node.data, 'right_child:', right_node.data)
            if(left_node == None):
                left_node = get_child_node(lhs_table, attribute_table_map)
            if(right_node == None):
                right_node = get_child_node(rhs_table, attribute_table_map)
            join_query_node = build_tree_from_join_query(condition.value, [left_node, right_node])
            join_query_nodes.append(join_query_node)
        
        
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
        join_query_nodes.append(build_tree_from_join_query(query, children_list, clause='cartesian product '))
    
    print('giving dqn:', direct_query_nodes)
    root = add_root(select_clause, join_query_nodes, direct_query_nodes, group_by_clause)
    return root 
