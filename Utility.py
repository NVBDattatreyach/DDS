# from main import attribute_table_map

# input => direct query, join query
class Tree:
    def __init__(self):
        self.children = []
        self.data = None

# assumption that => Emp.sal format will be used
def build_tree_from_direct_query(table_name, queries, clause='select '):
    # if(queries != None):
    #     parts = queries[0]
    # for parts in query:
    # print('part:', parts, 'type:', type(parts))
    child_node = Tree()
    # child_node.data = ((parts.value).split('.'))[0] # extracting table name from select operator
    child_node.data = table_name
    # child_node.left = None
    # child_node.right = None
    # print(table_name,'=>',queries)
    parent_node = Tree()
    parent_node.data = clause+queries
    parent_node.children.append(child_node)
    return parent_node

def build_tree_from_join_query(query, children_list, clause='join '):
    node = Tree()
    node.data = clause+query
    for child in children_list:
        node.children.append(child)
    return node

def add_root(select_tokens, join_query_nodes, direct_query_nodes, group_by_clause):
    
    final_project_node = Tree()
    final_project_node.data = 'project '+','.join(token for token in select_tokens)
    cur_node = final_project_node

    if(len(group_by_clause) > 0):
        # group_by_attrs = group_by_clause.split(' ', 1)[1].split(',')
        gp_by_node = Tree()
        gp_by_node.data = 'group by '+','.join(group_by_clause)
        final_project_node.children.append(gp_by_node)
        cur_node = gp_by_node
    
    for join_query_node in join_query_nodes:
        cur_node.children.append(join_query_node)
    for direct_query_node in direct_query_nodes:
        cur_node.children.append(direct_query_node)
    
    # if(len(join_query_nodes)==0 and len(direct_query_nodes)==0):
    #     child_node = Tree()
    #     child_node.data = ' '.join(table for table in from_clause)
    #     root_node.children.append(child_node)
    
    return final_project_node


def print_tree(root, space_count=1):
    if(root == None):
        return
    for i in range(0, space_count):
        print(' ', end='')
    print('-',root.data)
    for child in root.children:
        print_tree(child, space_count+1)

def break_query(condition):
    part_of_a_condition = []
    for part in condition:
        if(part.value == ' '):
            continue
        else:
            part_of_a_condition.append(part)
    return part_of_a_condition

def get_attribute_to_table_mapping(select_clause, from_clause):
    attribute_table_name_map = {}
    for attribute in select_clause:
        if('.' in attribute):
            table_name, attr = attribute.split('.')
            attribute_table_name_map[attr] = table_name
        else:
            for table_name in from_clause:
                attr_list = get_attr_list_for_table(table_name)
                if(attribute in attr_list):
                    attribute_table_name_map[attribute] = table_name
                    break
    
    return attribute_table_name_map

def get_attr_list_for_table(table_name):
    # tbd
    if(table_name == 'Emp'):
        return ['sal', 'Eno', 'EMP_ID']
    if(table_name == 'STUDENT'):
        return ['Student_ID']
    if(table_name == 'startsIn'):
        return ['title', 'yearr', 'startName']
    if(table_name == 'Movies'):
        return ['title', 'yearr', 'studioName']
    if(table_name == 'MovieStar'):
        return ['name', 'addr', 'gender', 'birthdate']
    if(table_name == 'StarsIn'):
        return ['movieTitle', 'movieYear', 'starName']
    return '*'

def get_table_name(attribute, from_clause, attribute_table_map): 
    if('.' in attribute):
        table_name = (attribute.split('.'))[0]
    else:
        if(attribute in attribute_table_map):
            table_name = attribute_table_map[attribute]
        else:
            res = (get_attribute_to_table_mapping([attribute], from_clause))
            if(attribute in res):
                table_name = res[attribute]
            else:
                table_name = None
    
    return table_name

def get_attribute_name(operand):
    if('.' in operand):
        return operand.split('.')[1]
    return operand

def split_query(query):
    operators = ['=','<','<=','>','>=','<>']
    for operator in operators:
        if(operator in query):
            return query.split(operator)

def get_optimized_tree(root, from_clause, attribute_table_map, table_attr_map):
    if(root == None):
        return
    # print('root:', root, ' table_attr_map:', table_attr_map)
    # leaf node case
    if(len(root.children) == 0):
        if(root.data.startswith('project')):
            attr_list = (root.data.split(' '))[1:]
            child_nodes = []
            table_names = []
            for attr in attr_list:
                table_name = get_table_name(attr, from_clause, attribute_table_map)
                table_names.append(table_name)
                if(table_name not in table_attr_map):
                    table_attr_map[table_name] = []
                table_attr_map[table_name].append(attr)
            
            for table_name in table_names:
                queries = ','.join(attr for attr in table_attr_map[table_name])
                child_nodes.append(build_tree_from_direct_query(table_name, queries, clause='project '))
        
            if(len(table_names)>1):
                query = ' '.join(t for t in table_names)
                new_leaf = build_tree_from_join_query(query, child_nodes, clause='cartesian product ')
            else:
                new_leaf = child_nodes[0]
            # root.children.append(new_leaf)
            return new_leaf
        else:
            table_name = root.data
            print('table_attr_map', table_attr_map)
            if(table_name in table_attr_map):
                queries = ','.join(attr for attr in table_attr_map[table_name])
            else:
                queries = '*'
            new_leaf = build_tree_from_direct_query(table_name, queries, clause='project ')
            # new_leaf.children.append(root)
            return new_leaf
    else:
        # print('splitting:', root.data.split(' ',1), 'root:', root.data)
        query_list = (root.data.split(' ',1))[1].split(',')
        query_type = root.data.split()[0]
        # print('q_lst:', query_list, 'q_type:', query_type)
        attr_list = []
        for query in query_list:
            if(query_type == 'project'):
                if(query == '*'):
                    continue
                attr_list.append(query)
            elif(query_type=='select' or query_type=='join'):
                # print('query:', query)
                left_operand, right_operand = split_query(query)
                attr_list.append(left_operand.strip())
                attr_list.append(right_operand.strip())
        for attr in attr_list:
            table_name = get_table_name(attr, from_clause, attribute_table_map)
            if(table_name != None):
                table_name = table_name.strip()
                if(table_name not in table_attr_map):
                    table_attr_map[table_name] = []
                attr_name = get_attribute_name(attr)
                if(attr_name not in table_attr_map[table_name]):
                    table_attr_map[table_name].append(attr_name)

        
        for i, child in enumerate(root.children):
            # print('child:', child, 'i:',i)
            # print('root:', root.data, 'table_attr_map:', table_attr_map)
            root.children[i] = get_optimized_tree(child, from_clause, attribute_table_map, table_attr_map)
        
        return root

def get_child_node(given_table_name, attribute_table_map):
    for attr, table_name in attribute_table_map.items():
        if(given_table_name == table_name):
            node = build_tree_from_direct_query(table_name, attr, clause='project ')
            return node
    node = build_tree_from_direct_query(table_name, '*', clause='project ')
    return node

def valid_group_by(group_by_clause, functions):
    # print('gp_attr:', group_by_clause)
    # gp_attrs = (group_by_clause.split(' ', 1)[1]).split(',')
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
            return False
    
    for aggregate_attrs in func_attrs_map.values():
        if(aggregate_attrs not in group_by_clause):
            return False
    
    return True