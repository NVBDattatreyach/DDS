# DDS
Implementation of distributed database system

Installations
- pip install pymysql

How To Run:
Paste the query inside main function and run
```
python main.py
```


File Structure:
- main.py: the main file, which calls all the functionalitites
- QueryParser.py: parses the query and identifies various clauses present in the query
- QueryDecomposer.py: creates a basic tree using relational algebra
- Utility: contains all the utility functions, including the optimization of the query tree
- localization.py: performs localization on the optimized query tree and generates a localized query tree.
- TableHandler.py: fetches column names for all the tables in application database.
- 2PC.py: imlements flask server for 2 phase commit protocol
- Coordinator.py: implements the coordinator part of the 2 phase commit protocol
- send_server.py: implements helper function to update the server code at all sites automatically
- newfile.txtx: logs 2 phase commit process
