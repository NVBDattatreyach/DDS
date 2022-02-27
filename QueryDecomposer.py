from Utility import *
import sqlparse
from QueryParser import parse_query
# from QueryParser import select_tokens, from_clause, where_clause




def decompose_query(select_clause, from_clause, where_clause):

    join_query = []
    direct_query = []
    direct_query_nodes = []
    join_query_nodes = []
    condition_concat = None

    print('select_clause', select_clause)
    print('from_clause', from_clause)
    print('where_clause', where_clause)

    for condition in where_clause:

        if(len(where_clause) == 0):
            return None

        left_part, comparison, right_part = break_query(condition)
        # print('left_part:', left_part, type(left_part))
        # print('comparison:', comparison, type(comparison))
        # print('right_part:', right_part, type(right_part))

        if(isinstance(right_part, sqlparse.sql.Parenthesis)):
            # print('right_part:', right_part)
            # for t in right_part:
            #     print(t, type(t))
            sel_clause, fr_clause, whr_clause, keyword = parse_query(right_part)
            subtree = decompose_query(sel_clause, fr_clause, whr_clause)
            direct_query_nodes.append(subtree)
        
        elif(type(right_part.value) == float or type(right_part.value) == str or type(right_part.value) == int):
            print('--------------- here1 -----------------')
            direct_query.append(condition)
            direct_query_node = build_tree_from_direct_query(condition)
            direct_query_nodes.append(direct_query_node)
        
        elif(isinstance(condition[0], sqlparse.sql.Identifier) and isinstance(condition[2], sqlparse.sql.Identifier)):
            print('--------------- here2 -----------------')
            join_query.append(condition)
            lhs_table = ((condition[0].value).split('.'))[0]
            rhs_table = ((condition[2].value).split('.'))[0]
            # print('lhs:', lhs_table, 'rhs:', rhs_table)

            left_node = None
            right_node = None

            # scan the direct_query_nodes list to find out the correct pair of nodes to be joined
            # also assuming that only one attribute is used to join 2 tables
            for idx, query in enumerate(direct_query_nodes):
                table_name = (((query.data.split())[1]).split('.'))[0]
                # print('direct query data:', query.data, 'table_name:', table_name)
                # print('table_name:', table_name, 'lhs_table:', lhs_table, 'rhs_table:', rhs_table)
                if(table_name == lhs_table):
                    left_node = query
                    direct_query_nodes.pop(idx)
                elif(table_name == rhs_table):
                    right_node = query
                    direct_query_nodes.pop(idx)
            # if(left_node!=None and right_node!=None):
                # print('left_child:', left_node.data, 'right_child:', right_node.data)
            join_query_node = build_tree_from_join_query(condition, left_node, right_node)
            join_query_nodes.append(join_query_node)
    
    
    '''
    print('select_tokens', select_tokens)
    print('from_clause', from_clause)
    print('where_clause', where_clause)


    for condn in where_clause:
        # print(condn, type(condn[2]))
        if(isinstance(condn[0], sqlparse.sql.Identifier) and isinstance(condn[2], sqlparse.sql.Token)):
            direct_query.append(condn)
            # print('appending:', condn)
            direct_query_node = build_tree_from_direct_query(condn)
            direct_query_nodes.append(direct_query_node)

    print('===== printing direct query nodes ======')
    for q in direct_query_nodes:
        print(q.data)
    print('=========================================')
    '''
    # for condn in where_clause:
        # print(condn)
        # assuming that 'and' is used to concat the conditions = (pVq) and (rVs)
        # if isinstance(condn, sqlparse.sql.Token) and (condition.value.lower()=='and' or condition.value.lower()=='or'):
        #     condition_concat = condition.value.lower()
        
        
    '''
    print('------direct queries---------')
    for query in direct_query_nodes:
        print(query.data)

    print('------join queries---------')
    for query in join_query_nodes:
        print(query.data)
    '''
    # appending root to this subtree
    # print('++++++++++++++++++++++++++++++++++++++')
    # print('direct queries:', direct_query_nodes)
    # print('join queries:', join_query_nodes)
    # print('++++++++++++++++++++++++++++++++++++++')
    root = add_root(select_clause, join_query_nodes, direct_query_nodes)
    # print('##############################')
    # print_tree(root) 
    # print('##############################')
    return root 

    ''' 
    print('keywords', keyword)
    print('join_query:', join_query)
    print('direct_query:', direct_query)

    for query in direct_query:
        for parts in query:
            print('part:', parts, 'type:', type(parts))
    '''
    