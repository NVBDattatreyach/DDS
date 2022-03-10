from QueryParser import parse_query
from QueryDecomposer import decompose_query
from Utility import *
import localization as Loc


clause_dict = parse_query('Select movieYear, max(birthdate) from MovieStar, StarsIn where name=starName group by movieYear having birthdate > 10')
attribute_table_map = get_attribute_to_table_mapping(clause_dict['select'], clause_dict['from'])
root = decompose_query(clause_dict, attribute_table_map)

print('########### Normal Tree ###############')
print_tree(root)
print('########################################')
optimized_tree = get_optimized_tree(root, clause_dict['from'], attribute_table_map, {})
print('############# Optimized Tree ###############')
print_tree(optimized_tree)
print('#############################################')
Loc.localize(optimized_tree)
print("After localization")
print_tree(optimized_tree)