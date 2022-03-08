import sqlparse
from Utility import valid_group_by


def parse_query(query):
    if(isinstance(query, sqlparse.sql.Parenthesis)):
        parsed_tokens = query
    else:
        parsed = sqlparse.parse(query)
        parsed_tokens = parsed[0]
    
    # parsed_tokens._pprint_tree()
    clause_name = None
    select_tokens = []
    from_clause = []
    where_clause = []
    group_by_clause = []
    functions = []
    for token in parsed_tokens:
        if(isinstance(token, sqlparse.sql.Token)):
            # print('token_val:', token.value.lower())
            if(token.value.lower() == 'select'):
                clause_name = 'select'
            elif(token.value.lower() == 'from'):
                clause_name = 'from'
            elif(token.value == '*'):
                select_tokens.append(token.value)
            elif(token.value.lower() == 'group by'):
                clause_name = 'group by'

        if(isinstance(token, sqlparse.sql.Where)):
            for condition in token.tokens:
                if isinstance(condition, sqlparse.sql.Comparison):
                    where_clause.append(condition)
        
        if(isinstance(token, sqlparse.sql.Identifier)):
            print('clause_name:', clause_name)
            if(clause_name == 'select'):
                select_tokens.append(token.value)
            elif(clause_name == 'from'):
                from_clause.append(token.value)
            elif(clause_name == 'group by'):
                group_by_clause.append(token.value)
        
        if(isinstance(token, sqlparse.sql.IdentifierList)):
            for identifier in token.get_identifiers():
                if(clause_name == 'select'):
                    select_tokens.append(identifier.value)
                elif(clause_name == 'from'):
                    from_clause.append(identifier.value)
        
        if(isinstance(token, sqlparse.sql.Function)):
            select_tokens.append(token.value)
            functions.append(token)
    print('gp_clause:', group_by_clause)    
    if(valid_group_by(group_by_clause, functions) == False):
        print('not a valid query')
    
    return select_tokens, from_clause, where_clause, group_by_clause
