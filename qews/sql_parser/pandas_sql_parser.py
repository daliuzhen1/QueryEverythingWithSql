#csv_sql_parser.py
import pandas as pd
import apsw
class PandasSourceInfo:
    def __init__(self, data_frame):
            self.data_frame = data_frame

class PandasRegisteTableInfo:
    def __init__(self, table_name, pandas_source_info):
        self.table_name = table_name
        self.pandas_source_info = pandas_source_info

class PandasModule:
    def __init__(self):
        self.table_list = {}

    def registeTable(self, pandas_resgiste_table_info):
        self.table_list[pandas_resgiste_table_info.table_name] = PandasTable(pandas_resgiste_table_info)

    def Create(self, db, modulename, dbname, tablename, *args):
        return self.table_list[tablename].declareTable()

    Connect = Create


class PandasTable:
    def __init__(self, pandas_resgiste_table_info):
        self.pandas_resgiste_table_info = pandas_resgiste_table_info
        self.pd_data_frame = self.pandas_resgiste_table_info.pandas_source_info.data_frame
    
    def declareTable(self):
        df = self.pd_data_frame
        schema="create table X("+','.join(["'%s'" % (x,) for x in df.columns.tolist()])+")"
        return schema, self

    def BestIndex(self, *args):
        return None

    def Open(self):
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
