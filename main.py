from QueryParser import parse_query
from QueryDecomposer import decompose_query
from Utility import *

select_clause, from_clause, where_clause, keyword = parse_query('Select * from Student Where classID = (Select id from class where noofstudents = (select max(noofstudents) from class))')
attribute_table_map = get_attribute_to_table_mapping(select_clause, from_clause)
root = decompose_query(select_clause, from_clause, where_clause, attribute_table_map)
print('########### Normal Tree ###############')
print_tree(root)
print('########################################')
optimized_tree = get_optimized_tree(root, from_clause, attribute_table_map, {})
print('############# Optimized Tree ###############')
print_tree(optimized_tree)
print('#############################################')