# DDS
Implementation of distributed database system

Installations
- pip install pymysql

How to run
- python .\main.py "<query>". All the string constants should be in uppercase. 
(Use single quotes without escape character. Give all the attribute names in the format <Table_name.attribute_name>)

File Structure:
- main.py: the main file, which calls all the functionalitites
- QueryParser.py: parses the query and identifies various clauses present in the query
- QueryDecomposer.py: creates a basic tree using relational algebra
- Utility: contains all the utility functions, including the optimization of the query tree
- localization.py: performs localization on the optimized query tree and generates a localized query tree.
- TableHandler.py: fetches column names for all the tables in application database.