from QueryParser import parse_query
from QueryDecomposer import decompose_query
from Utility import *

select_clause, from_clause, where_clause, group_by_clause = parse_query('Select EMPLOYEE_DETAILS.Age from EMPLOYEE,EMPLOYEE_DETAILS where EMPLOYEE.Emp_Id=EMPLOYEE_DETAILS.Emp_Id and EMPLOYEE.Dept_Name=\'SALES\'')
attribute_table_map = get_attribute_to_table_mapping(select_clause, from_clause)
root = decompose_query(select_clause, from_clause, where_clause, group_by_clause, attribute_table_map)
print('########### Normal Tree ###############')
print_tree(root)
print('########################################')
optimized_tree = get_optimized_tree(root, from_clause, attribute_table_map, {})
print('############# Optimized Tree ###############')
print_tree(optimized_tree)
print('#############################################')
