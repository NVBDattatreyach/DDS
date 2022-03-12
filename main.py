from QueryParser import parse_query
from QueryDecomposer import decompose_query
from Utility import *
# import localization as Loc

query_to_alias={}

clause_dict, condition_concat = parse_query('Select * from EMPLOYEE , EMPLOYEE_DETAILS where EMPLOYEE.Emp_Id=EMPLOYEE_DETAILS.Emp_Id and EMPLOYEE.Dept_Name=\'SALES\'')
attribute_table_map = get_attribute_to_table_mapping(clause_dict['select'], clause_dict['from'])
# print('MAIN:', condition_concat)
root = decompose_query(clause_dict, condition_concat, attribute_table_map)

print('########### Normal Tree ###############')
print_tree(root)
print('########################################')
optimized_tree = get_optimized_tree(root, clause_dict['from'], attribute_table_map, {})
print('############# Optimized Tree ###############')
print_tree(optimized_tree)
print('#############################################')
'''
Loc.localize(optimized_tree)
print("After localization")
print_tree(optimized_tree)
'''