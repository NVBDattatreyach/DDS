import sqlparse
# from Utility import valid_group_by

class QueryParser:

    # --------------------------------------- initializing data structures -------------------------
    def __init__(self, query):
        self.query = query
        self.clause_name = None
        self.parsed_tokens = None

        self.clause_dict = {}
        self.clause_dict['select'] = []
        self.clause_dict['from'] = []
        self.clause_dict['where'] = []
        self.clause_dict['group by'] = []
        self.clause_dict['having'] = []
        self.clause_dict['functions'] = []

        self.condition_concat = {}
        self.condition_concat['and'] = []
        self.condition_concat['or'] = []
    

    # ---------------------------------------- parsing the input query ------------------------------
    def get_parsed_tokens(self):
        if(isinstance(self.query, sqlparse.sql.Parenthesis)):
            self.parsed_tokens = self.query
        else:
            parsed = sqlparse.parse(self.query)
            self.parsed_tokens = parsed[0]
    

    # ----------------------------------------- print query tree --------------------------------------
    def print_query_tree(self):
        self.parsed_tokens._pprint_tree()
    

    # ----------------------- Get different types of clauses from input query -------------------------
    def get_clauses(self):
        clause_name = None
        for token in self.parsed_tokens:
            print(token.value, type(token))
            
            if(isinstance(token, sqlparse.sql.Token)):
                if(token.value.lower() == 'select'):
                    clause_name = 'select'
                elif(token.value.lower() == 'from'):
                    clause_name = 'from'
                elif(token.value == '*'):
                    self.clause_dict['select'].append(token.value)
                elif(token.value.lower() == 'group'):
                    clause_name = 'group by'
                elif(token.value.lower() == 'having'):
                    clause_name = 'having'
            # print('clause_name:', clause_name)

            if(isinstance(token, sqlparse.sql.Where)):
                cur_concat = None
                prev_condn = None

                for condition in token.tokens:
                    if(condition.value.lower() == ' ' or condition.value.lower() == 'where'):
                        continue
                    if isinstance(condition, sqlparse.sql.Comparison):
                        self.clause_dict['where'].append(condition)
                        if(cur_concat!=None):
                            self.condition_concat[cur_concat].append(condition)
                        prev_condn = condition
                    elif(condition.value.lower() == 'and'):
                        if(cur_concat!=None and len(token_lst)>0):
                            # print('key:',cur_concat, 'token lst:', token_lst)
                            condn = sqlparse.sql.Comparison(token_lst)
                            self.clause_dict['where'].append(condn)
                            self.condition_concat[cur_concat].append(condn)
                            prev = condn
                        
                        self.condition_concat['and'].append(prev_condn)
                        token_lst = []
                        cur_concat = 'and'
                    elif(condition.value.lower() == 'or'):
                        if(cur_concat!=None and len(token_lst)>0):
                            # print('key:',cur_concat, 'token lst:', token_lst)
                            condn = sqlparse.sql.Comparison(token_lst)
                            self.clause_dict['where'].append(condn)
                            self.condition_concat[cur_concat].append(condn)
                            prev_condn = condn
                        self.condition_concat['or'].append(prev_condn)
                        token_lst = []
                        cur_concat = 'or'
                    else:
                        print('this condn:', condition)
                        token_lst.append(condition)
                if(cur_concat!=None and len(token_lst)>0):
                    # print('key:',cur_concat, 'token lst:', token_lst)
                    condn = sqlparse.sql.Comparison(token_lst)
                    self.clause_dict['where'].append(condn)
                    self.condition_concat[cur_concat].append(condn)
                
                

            if(isinstance(token, sqlparse.sql.Identifier)):
                if(clause_name == 'select'):
                    self.clause_dict['select'].append(token.value)
                elif(clause_name == 'from'):
                    self.clause_dict['from'].append(token.value)
                elif(clause_name == 'group by'):
                    self.clause_dict['group by'].append(token.value)
                elif(clause_name == 'having'):
                    self.clause_dict['having'].append(token.value)
            
            if(isinstance(token, sqlparse.sql.IdentifierList)):
                for identifier in token.get_identifiers():
                    if(clause_name == 'select'):
                        self.clause_dict['select'].append(identifier.value)
                    elif(clause_name == 'from'):
                        self.clause_dict['from'].append(identifier.value)
        
            if(isinstance(token, sqlparse.sql.Comparison)):
                if(clause_name == 'having'):
                    self.clause_dict['having'].append(token.value)
            
            if(isinstance(token, sqlparse.sql.Function)):
                self.clause_dict['select'].append(token.value)
                self.clause_dict['functions'].append(token)
            
            

    
    # ------------------------------ check for a valid group by clause --------------------------------
    def is_valid_group_by(self):
        group_by_clause, functions = self.clause_dict['group by'], self.clause_dict['functions']
        func_attrs_map = {}
        for func in functions:
            for token in func:
                if(isinstance(token, sqlparse.sql.Identifier)):
                    func_name = token
                elif(isinstance(token, sqlparse.sql.Parenthesis)):
                    for part in token.tokens():
                        if(isinstance(part, sqlparse.sql.Identifier)):
                            attr_name = part
            if(func_name!=None and attr_name!=None):
                func_attrs_map[func_name] = attr_name
            else:
                print('invalid group by')
        
        for aggregate_attrs in func_attrs_map.values():
            if(aggregate_attrs not in group_by_clause):
                print('invalid group by')

    # ------------------------------- driver function ----------------------------------------
    def parse_query(self):
        self.get_parsed_tokens()
        self.print_query_tree()
        self.get_clauses()
        self.is_valid_group_by()
    