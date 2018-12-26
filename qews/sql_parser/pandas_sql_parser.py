#csv_sql_parser.py
import pandas as pd
import apsw

class PandasTableSourceInfo:
    def __init__(self, table_name, data_frame):
        self.table_name = table_name
        self.data_frame = data_frame

class PandasModule:
    def __init__(self, module_name):
        self.table_list = {}
        self.module_name = module_name

    def createTable(self, cursor, pandas_table_source_info):
        self.table_list[pandas_table_source_info.table_name] = PandasTable(pandas_table_source_info)
        cursor.execute("create virtual table "+ pandas_table_source_info.table_name +" using pandas")

    def Create(self, db, modulename, dbname, tablename, *args):
        return self.table_list[tablename].declareTable()

    Connect = Create


class PandasTable:
    def __init__(self, pandas_table_source_info):
        self.pandas_table_source_info = pandas_table_source_info
        self.pd_data_frame = self.pandas_table_source_info.data_frame
    
    def declareTable(self):
        print ("declareTable")
        df = self.pd_data_frame
        schema="create table X("+','.join(["'%s'" % (x,) for x in df.columns.tolist()])+")"
        return schema, self

    def BestIndex(self, *args):
        print ("BestIndex")
        return None

    def Open(self):
        print ("Open")
        return PandasCursor(self)

    def Disconnect(self):
        pass

    Destroy=Disconnect

class PandasCursor:
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
