# input => direct query, join query
class Tree:
    def __init__(self):
        self.children = []
        self.data = None

# assumption that => Emp.sal format will be used
def build_tree_from_direct_query(query):
    parts = query[0]
    # for parts in query:
    # print('part:', parts, 'type:', type(parts))
    child_node = Tree()
    child_node.data = ((parts.value).split('.'))[0] # extracting table name from select operator
    # child_node.left = None
    # child_node.right = None
        
    parent_node = Tree()
    parent_node.data = 'select '+parts.value
    parent_node.children.append(child_node)
    return parent_node

def build_tree_from_join_query(query, left_child, right_child):
    node = Tree()
    node.data = 'join '+query.value
    node.children.append(left_child)
    node.children.append(right_child)
    return node

def add_root(select_tokens, join_query_nodes, direct_query_nodes):
    root_node = Tree()
    root_node.data = 'project '+' '.join(token for token in select_tokens)
    # print('len:', len(direct_query_nodes))
    # print('child len1:', len(root_node.children))
    for join_query_node in join_query_nodes:
        # print('999999999999999999')
        root_node.children.append(join_query_node)
    # print('child len2:', len(root_node.children))
    for direct_query_node in direct_query_nodes:
        # print('direct_query_node === ', direct_query_node.data)
        root_node.children.append(direct_query_node)
    
    return root_node


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