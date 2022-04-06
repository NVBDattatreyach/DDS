import pandas as pd
import sys
from PopulateTables import TableFiller

filenames = [sys.argv[1]]

for filename in filenames:
    data = pd.read_csv (filename+'.csv')   
    df = pd.DataFrame(data)
    # df.drop(df.columns[[0]], axis=1, inplace=True)
    print(df)

    table_filler = TableFiller(df)
    table_filler.populate_table(filename)