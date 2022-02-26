# input => direct query, join query
class Tree:
    def __init__(self):
        self.left = None
        self.right = None
        self.data = None

# assumption that => Emp.sal format will be used
def build_tree_from_direct_query(query):
    parts = query[0]
    # for parts in query:
    # print('part:', parts, 'type:', type(parts))
    child_node = Tree()
    child_node.data = ((parts.value).split('.'))[0] # extracting table name from select operator
    child_node.left = None
    child_node.right = None
        
    parent_node = Tree()
    parent_node.data = 'select '+parts.value
    parent_node.left = child_node
    parent_node.right = None
    return parent_node

def build_tree_from_join_query(query, left_child, right_child):
    node = Tree()
    node.data = 'join '+query.value
    node.left = left_child
    node.right = right_child
    return node

def print_tree(root, space_count=1):
    if(root == None):
        return
    for i in range(0, space_count):
        print(' ', end='')
    print('-',root.data)
    print_tree(root.left, space_count+1)
    print_tree(root.right, space_count+1)