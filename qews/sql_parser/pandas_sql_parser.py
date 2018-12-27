#csv_sql_parser.py
import pandas as pd
import apsw
import json

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

    def BestIndex(self, constraints, orderbys):
        
        if len(constraints) == 0:
            return None
        fillter_json_array = []
        ret_constraint_used  = ()
        for index in range(len(constraints)):
            ret_constraint_used = ret_constraint_used + (index,)
            fillter_json = {"col_index":constraints[index][0], "operation" : constraints[index][1]}
            fillter_json_array.append(fillter_json)
        fillter_json_array = json.dumps(fillter_json_array)
        print (self.pandas_table_source_info.table_name)
        return ret_constraint_used, 0, fillter_json_array,True,1000


    def Open(self):
        print ("Open")
        return PandasCursor(self)

    def Disconnect(self):
        pass

    Destroy=Disconnect

class PandasCursor:
    def __init__(self, table):
        self.table = table
        self.sort_data_frame = false

    def Filter(self, indexnum, filter_json, constraintargs):
        filter_infomation = json.load(filter_json)
        col_indexs = []
        for i in range(len(filter_infomation))：
            col_indexs.append(filter_infomation[i]["col_index"])
        
        self.pos = 0

    def Eof(self):
        return self.pos >= len(self.table.pd_data_frame.values)

    def Rowid(self):
        return self.table.data[self.pos][0]

    def Column(self, col):
        # print ("Column")
        # print (col)
        if self.table.pd_data_frame.dtypes[col] == "object":
           return str(self.table.pd_data_frame.values[self.pos][col])
        return self.table.pd_data_frame.values[self.pos][col]

    def Next(self):
        # print (slf.pos)
        self.pos += 1
        return

    def Close(self):
        pass
