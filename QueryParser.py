import sqlparse
from QueryDecomposer import *

def parse_query(query):
    parsed = sqlparse.parse(query)[0]
    # parsed._pprint_tree()
    # print('type of parsed:', type(parsed))
    clause_name = None
    select_tokens = []
    from_clause = []
    where_clause = []
    keyword = []
    for token in parsed.tokens:
        # print('parent:', token.get_parent_name())
        if(isinstance(token, sqlparse.sql.Token)):
            if(token.value.lower() == 'select'):
                clause_name = 'select'
            elif(token.value.lower() == 'from'):
                clause_name = 'from'

        if(isinstance(token, sqlparse.sql.Where)):
            for condition in token.tokens:
                # print('cond:', condition, 'type:', type(condition))
                if isinstance(condition, sqlparse.sql.Comparison):
                    where_clause.append(condition)
                    # print('comparison ===', condition)
                    # print('iden tokens:', condition.tokens)
                    # if isinstance(condition.tokens, sqlparse.sql.Identifier):
                    #     print('identifier ===', condition)
                if isinstance(condition, sqlparse.sql.Token) and (condition.value.lower()=='and' or condition.value.lower()=='or'):
                    keyword.append(condition.value)
        
        if(isinstance(token, sqlparse.sql.Identifier)):
            if(clause_name == 'select'):
                select_tokens.append(token.value)
            elif(clause_name == 'from'):
                from_clause.append(token.value)
        
        if(isinstance(token, sqlparse.sql.IdentifierList)):
            for identifier in token.get_identifiers():
                if(clause_name == 'select'):
                    select_tokens.append(identifier.value)
                elif(clause_name == 'from'):
                    from_clause.append(identifier.value)
    return select_tokens, from_clause, where_clause, keyword

select_tokens, from_clause, where_clause, keyword = parse_query('Select Eno From Emp, Dept Where Emp.sal>50.85 and Dept.loc=\'Hyd\' and Emp.Dno=Dept.Dno')
print('select_tokens', select_tokens)
# print('from_clause', from_clause)
# print('where_clause', where_clause)

join_query = []
direct_query = []
direct_query_nodes = []
join_query_nodes = []
condition_concat = None

for condn in where_clause:
    # print(condn)
    # assuming that 'and' is used to concat the conditions
    # if isinstance(condn, sqlparse.sql.Token) and (condition.value.lower()=='and' or condition.value.lower()=='or'):
    #     condition_concat = condition.value.lower()
    if(isinstance(condn[0], sqlparse.sql.Identifier) and isinstance(condn[2], sqlparse.sql.Identifier)):
        join_query.append(condn)
        lhs_table = ((condn[0].value).split('.'))[0]
        rhs_table = ((condn[2].value).split('.'))[0]
        # print('lhs:', lhs_table, 'rhs:', rhs_table)

        left_node = None
        right_node = None

        # scan the direct_query_nodes list to find out the correct pair of nodes to be joined
        # also assuming that only one attribute is used to join 2 tables
        for idx, query in enumerate(direct_query_nodes):
            table_name = (((query.data.split())[1]).split('.'))[0]
            # print('direct query data:', query.data, 'table_name:', table_name)
            if(table_name == lhs_table):
                left_node = query
                direct_query_nodes.pop(idx)
            elif(table_name == rhs_table):
                right_node = query
                direct_query_nodes.pop(idx)
        # print('left_child:', left_node.data, 'right_child:', right_node.data)
        join_query_node = build_tree_from_join_query(condn, left_node, right_node)
        join_query_nodes.append(join_query_node)
    
    elif(isinstance(condn[0], sqlparse.sql.Identifier) and isinstance(condn[2], sqlparse.sql.Token)):
        direct_query.append(condn)
        direct_query_node = build_tree_from_direct_query(condn)
        direct_query_nodes.append(direct_query_node)

# appending root to this subtree
root = add_root(select_tokens, join_query_nodes)
print_tree(root)  
''' 
print('keywords', keyword)
print('join_query:', join_query)
print('direct_query:', direct_query)

for query in direct_query:
    for parts in query:
        print('part:', parts, 'type:', type(parts))
'''