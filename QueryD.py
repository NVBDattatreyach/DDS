import sqlparse
from Utility import *
from QueryP import QueryParser

class QueryDecomposer:

    # ----------------------------- initializing data structure and extracting clauses -------------------------
    def __init__(self, clause_dict, condition_concat, attribute_table_map):

        self.join_query = []
        self.direct_query = []
        self.direct_query_nodes = []
        self.join_query_nodes = []
        self.direct_query = {}
        # condition_concat = None

        self.select_clause = clause_dict['select']
        self.from_clause = clause_dict['from']
        self.where_clause = clause_dict['where']
        self.group_by_clause = clause_dict['group by']
        self.having_clause = clause_dict['having']

        self.condition_concat = condition_concat
        self.attribute_table_map = attribute_table_map

        print('select_clause', self.select_clause)
        print('from_clause', self.from_clause)
        print('where_clause', self.where_clause)
        print('group_by', self.group_by_clause)
        print('having', self.having_clause)
    
    # ---------------------------------- extracting direct (select) queries -------------------------------
    def extract_direct_queries(self):

        for condition in self.where_clause:
            # print('condition ---', condition)
            left_part, comparison, right_part = break_query(condition)

            if(isinstance(right_part, sqlparse.sql.Parenthesis)):
                query_parser = QueryParser(right_part)
                sel_clause, fr_clause, whr_clause, gp_by_clause = query_parser.parse_query()
                query_decomposer = QueryDecomposer(sel_clause, fr_clause, whr_clause, gp_by_clause, attribute_table_map)
                subtree = query_decomposer.decompose_query() 
                self.direct_query_nodes.append(subtree)
            
            
            elif(isinstance(condition[0], sqlparse.sql.Identifier) and isinstance(condition[2], sqlparse.sql.Identifier)):
                continue
            
            else:
                table_name = get_table_name(left_part.value, self.from_clause, self.attribute_table_map)
                if(table_name not in self.direct_query):
                    self.direct_query[table_name] = []
                self.direct_query[table_name].append(condition.value)
    

    # -------------------------------- join queries using 'AND' / 'OR' keyword -------------------------
    def concatenate_direct_queries(self):
        for table_name, queries in self.direct_query.items():
            query = None
            for idx, q in enumerate(queries):
                if(idx == 0):
                    query = q
                    prev = q
                elif(find_concat_keyword(self.condition_concat['and'], q, prev)):
                    query = query + ' and ' + q
                    prev = q
                else:
                    query = query + ' or ' + q
                    prev = q
                
            
            direct_query_node = build_tree_from_direct_query(table_name, query)
            self.direct_query_nodes.append(direct_query_node)
    

    def extract_join_queries(self):
        for condition in self.where_clause:

            if(len(self.where_clause) == 0):
                return None

            if(isinstance(condition[0], sqlparse.sql.Identifier) and isinstance(condition[2], sqlparse.sql.Identifier)):
                self.join_query.append(condition)

                lhs_table = get_table_name(condition[0].value, self.from_clause, self.attribute_table_map)
                rhs_table = get_table_name(condition[2].value, self.from_clause, self.attribute_table_map)

                left_node = None
                right_node = None

                # scan the direct_query_nodes list to find out the correct pair of nodes to be joined
                for idx, query in enumerate(self.direct_query_nodes):
                    if(query == None):
                        continue
                    q_l, q_r = split_query(query.data.split(' ',1)[1])
                    table_name = get_table_name(q_l.strip(), self.from_clause, self.attribute_table_map)
                    if(table_name == lhs_table):
                        left_node = query
                        self.direct_query_nodes[idx] = None
                    elif(table_name == rhs_table):
                        right_node = query
                        self.direct_query_nodes[idx] = None
                        
                if(left_node == None):
                    attr = get_attr(condition[0].value)
                    left_node = get_child_node(lhs_table, attr, self.attribute_table_map)
                if(right_node == None):
                    attr = get_attr(condition[2].value)
                    right_node = get_child_node(rhs_table, attr, self.attribute_table_map)
                join_query_node = build_tree_from_join_query(condition.value, [left_node, right_node])
                self.join_query_nodes.append(join_query_node)
        
        for query in self.direct_query_nodes:
            lst = []
            if(query != None):
                lst.append(query)
            self.direct_query_nodes = lst
    
    def concatenate_join_queries(self):
        join_nodes = []
        # and_node = None
        for idx, join_query_node in enumerate(self.join_query_nodes):
            if(idx == 0):
                query = join_query_node
                # prev = join_query_node
                cur_node = join_query_node
                # cur_node = root
            elif(find_concat_keyword(self.condition_concat['and'], join_query_node.data.split('join ',1)[1], cur_node.data.split('join ',1)[1])):
                cur_node_children = cur_node.children
                cur_node.children = [join_query_node]

                all_children = []
                for child in join_query_node.children:
                    all_children.append(child.children[0].data)
                
                for child in cur_node_children:
                    if(child.children[0].data not in all_children):
                        cur_node.children.append(child)
                    elif(child.data.startswith('select')):
                        for idx, c in enumerate(join_query_node.children):
                            if(join_query_node.children[idx].children[0].data == child.children[0].data):
                                join_query_node.children[idx] = child

                join_query_node.parent = cur_node
                cur_node = join_query_node
                self.join_query_nodes.pop(idx)
                # and_node = root
                # prev = join_query_node
            else:
                query = query_to_alias[join_query_node.data] + ' UNION ' + query_to_alias[prev.data]
                join_nodes.append(build_tree_from_join_query(query, [join_query_node, prev], ''))
                # prev = join_query_node
        
        # if(and_node!=None):
        #     join_nodes.append(and_node)

        if(len(join_nodes)>0):
            self.join_query_nodes = join_nodes
        for node in self.join_query_nodes:
            print('node:', node.data)
            for child in node.children:
                print('child:', child.data)


    def create_generic_query(self):
        children_list = []
        for table in self.from_clause:
            child_node = Tree()
            child_node.data = table
            children_list.append(child_node)
        query = ','.join(table for table in self.from_clause)
        print('from_clause:', self.from_clause)
        if(len(self.from_clause)>1):
            self.join_query_nodes.append(build_tree_from_join_query(query, children_list, clause='cartesian product '))
        # else:
        #     self.join_query_nodes.append(build_tree_from_direct_query(self.from_clause[0], '*', clause='Project '))
    

    def decompose_query(self):
        if(len(self.where_clause) > 0):
            self.extract_direct_queries()
            self.concatenate_direct_queries()
            self.extract_join_queries()
            self.concatenate_join_queries()
        else:
            self.create_generic_query()
        
        root = add_root(self.select_clause, self.join_query_nodes, self.direct_query_nodes, self.group_by_clause, self.having_clause)
        return root 
        
    

