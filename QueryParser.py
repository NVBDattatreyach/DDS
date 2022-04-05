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

    condition_concat = {}
    condition_concat['and'] = []
    condition_concat['or'] = []
    
    
    for token in parsed_tokens:
        # print('token:', token, 'type:', type(token))
        if(isinstance(token, sqlparse.sql.Token)):
            if(token.value.lower() == 'select'):
                clause_name = 'select'
            elif(token.value.lower() == 'from'):
                clause_name = 'from'
            elif(token.value == '*'):
                clause_dict['select'].append(token.value)
            elif(token.value.lower() == 'group'):
                clause_name = 'group by'
            elif(token.value.lower() == 'having'):
                clause_name = 'having'
            

        if(isinstance(token, sqlparse.sql.Where)):
            cur_concat = None
            prev_condn = None

            for condition in token.tokens:
                if(condition.value.lower() == ' ' or condition.value.lower() == 'where'):
                    continue
                # print('CONDN ---', condition.value)
                if isinstance(condition, sqlparse.sql.Comparison):
                    clause_dict['where'].append(condition)
                    if(cur_concat!=None):
                        condition_concat[cur_concat].append(condition)
                    prev_condn = condition
                elif(condition.value.lower() == 'and'):
                    if(cur_concat!=None and len(token_lst)>0):
                        # print('key:',cur_concat, 'token lst:', token_lst)
                        condn = sqlparse.sql.Comparison(token_lst)
                        clause_dict['where'].append(condn)
                        condition_concat[cur_concat].append(condn)
                        prev = condn
                    
                    condition_concat['and'].append(prev_condn)
                    token_lst = []
                    cur_concat = 'and'
                elif(condition.value.lower() == 'or'):
                    if(cur_concat!=None and len(token_lst)>0):
                        # print('key:',cur_concat, 'token lst:', token_lst)
                        condn = sqlparse.sql.Comparison(token_lst)
                        clause_dict['where'].append(condn)
                        condition_concat[cur_concat].append(condn)
                        prev_condn = condn
                    condition_concat['or'].append(prev_condn)
                    token_lst = []
                    cur_concat = 'or'
                else:
                    print('this condn:', condition)
                    token_lst.append(condition)
            if(cur_concat!=None and len(token_lst)>0):
                # print('key:',cur_concat, 'token lst:', token_lst)
                condn = sqlparse.sql.Comparison(token_lst)
                clause_dict['where'].append(condn)
                condition_concat[cur_concat].append(condn)
            
                

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
    
    # for key, val in clause_dict.items():
    #     print(key,' => ', val)
    # print('concn_concat ===', condition_concat)
    return clause_dict, condition_concat
