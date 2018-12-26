#csv_sql_parser.py
import pandas as pd
from enum import IntEnum
import apsw
class CsvSourceInfo:
    def __init__(self, file_path, separator = ','):
            self.file_path = file_path

class CsvRegisteTableInfo:
    def __init__(self, table_name, csv_source_info):
        self.table_name = table_name
        self.csv_source_info = csv_source_info

class CsvModule:
    def __init__(self):
        self.table_list = {}

    def registeTable(self, csv_resgiste_table_info):
        self.table_list[csv_resgiste_table_info.table_name] = CsvTable(csv_resgiste_table_info)

    def Create(self, db, modulename, dbname, tablename, *args):
        return self.table_list[tablename].declareTable()

    Connect = Create


class CsvTable:
    def __init__(self, csv_resgiste_table_info):
        self.csv_resgiste_table_info = csv_resgiste_table_info
        self.pd_data_frame = None
    
    def declareTable(self):
        df = pd.read_csv(self.csv_resgiste_table_info.csv_source_info.file_path)
        schema="create table X("+','.join(["'%s'" % (x,) for x in df.columns.tolist()])+")"
        self.pd_data_frame = df
        return schema, self

    def BestIndex(self, *args):
        return None

    def Open(self):
        return CsvCursor(self)

    def Disconnect(self):
        pass

    Destroy=Disconnect

class CsvCursor:
    def __init__(self, table):
        self.table = table

    def Filter(self, *args):
        self.pos = 0

    def Eof(self):
        return self.pos >= len(self.table.pd_data_frame.values)

    def Rowid(self):
        return self.table.data[self.pos][0]

    def Column(self, col):
        if self.table.pd_data_frame.dtypes[col] == "object":
           return str(self.table.pd_data_frame.values[self.pos][col])
        return self.table.pd_data_frame.values[self.pos][col]

    def Next(self):
        self.pos += 1
        return

    def Close(self):
        pass

# csv_source_info = CsvSourceInfo("C:\\Users\\zhenl\\Documents\\yahoo_prices.csv")
# connection=apsw.Connection(":memory:")
# csvModule = CsvModule()
# csv_resgiste_table_info  = CsvRegisteTableInfo("yahoo_prices" , csv_source_info)
# csvModule.registeTable(csv_resgiste_table_info)
# connection.createmodule("csv", csvModule)
# cursor=connection.cursor()
# cursor.execute("create virtual table yahoo_prices using csv")
# data = cursor.execute("select Volume from yahoo_prices")
# print (data)
# for i in data:
#     print (i)
# data = pd.read_csv("C:\\Users\\zhenl\\Documents\\yahoo_prices.csv")
