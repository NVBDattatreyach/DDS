from QueryParser import parse_query
from QueryDecomposer import decompose_query
from Utility import *

select_tokens, from_clause, where_clause, keyword = parse_query('Select * from Student Where classID = (Select id from classs where noofstudents = (select max(noofstudents) from classs))')
root = decompose_query(select_tokens, from_clause, where_clause)
print_tree(root) 