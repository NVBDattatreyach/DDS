from QueryParser import parse_query
from QueryDecomposer import decompose_query
from Utility import *

select_tokens, from_clause, where_clause, keyword = parse_query('Select Eno From Emp, Dept Where Emp.sal>50 and Emp.Dno=Dept.Dno and Emp.fname=\'Ram\' and Dept.loc=\'Hyd\'')
root = decompose_query(select_tokens, from_clause, where_clause)
print_tree(root) 