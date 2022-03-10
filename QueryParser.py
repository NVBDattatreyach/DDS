import sqlparse
from Utility import valid_group_by


def parse_query(query):
    if(isinstance(query, sqlparse.sql.Parenthesis)):
        parsed_tokens = query
    else:
        parsed = sqlparse.parse(query)
        parsed_tokens = parsed[0]
    
    parsed_tokens._pprint_tree()
    clause_name = None
    clause_dict = {}
    clause_dict['select'] = []
    clause_dict['from'] = []
    clause_dict['where'] = []
    clause_dict['group by'] = []
    clause_dict['having'] = []
    clause_dict['functions'] = []
    
    
    for token in parsed_tokens:
        if(isinstance(token, sqlparse.sql.Token)):
            if(token.value.lower() == 'select'):
                clause_name = 'select'
            elif(token.value.lower() == 'from'):
                clause_name = 'from'
            elif(token.value == '*'):
                select_tokens.append(token.value)
            elif(token.value.lower() == 'group by'):
                clause_name = 'group by'
            elif(token.value.lower() == 'having'):
                clause_name = 'having'
            

        if(isinstance(token, sqlparse.sql.Where)):
            for condition in token.tokens:
                if isinstance(condition, sqlparse.sql.Comparison):
                    clause_dict['where'].append(condition)

        if(isinstance(token, sqlparse.sql.Identifier)):
            if(clause_name == 'select'):
                clause_dict['select'].append(token.value)
            elif(clause_name == 'from'):
                clause_dict['from'].append(token.value)
            elif(clause_name == 'group by'):
                clause_dict['group by'].append(token.value)
            elif(clause_name == 'having'):
                clause_dict['having'].append(token.value)
        
        if(isinstance(token, sqlparse.sql.IdentifierList)):
            for identifier in token.get_identifiers():
                if(clause_name == 'select'):
                    clause_dict['select'].append(identifier.value)
                elif(clause_name == 'from'):
                    clause_dict['from'].append(identifier.value)
        
        if(isinstance(token, sqlparse.sql.Comparison)):
            if(clause_name == 'having'):
                clause_dict['having'].append(token.value)
        
        if(isinstance(token, sqlparse.sql.Function)):
            clause_dict['select'].append(token.value)
            clause_dict['functions'].append(token)

    if(valid_group_by(clause_dict['group by'], clause_dict['functions']) == False):
        print('not a valid query')
    
    return clause_dict
