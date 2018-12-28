import pandas as pd
import apsw
import json

class PandasTableSourceInfo:
    def __init__(self, table_name, data_frame):
        self.table_name = table_name
        self.data_frame = data_frame

class PandasModule:
    def __init__(self, connection):
        self.table_list = {}
        self.module_name = "pandas"
        connection.createmodule(self.module_name, self)

    def createTable(self, cursor, pandas_table_source_info):
        self.table_list[pandas_table_source_info.table_name] = PandasTable(pandas_table_source_info)
        cursor.execute("create virtual table "+ pandas_table_source_info.table_name +" using " + self.module_name)

    def Create(self, db, modulename, dbname, tablename, *args):
        return self.table_list[tablename].declareTable()

    Connect = Create


class PandasTable:
    def __init__(self, pandas_table_source_info):
        self.pandas_table_source_info = pandas_table_source_info
        self.pd_data_frame = self.pandas_table_source_info.data_frame
    
    def declareTable(self):
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
        return ret_constraint_used, 0, fillter_json_array,False,1000


    def Open(self):
        return PandasCursor(self)

    def Disconnect(self):
        pass

    Destroy=Disconnect

class FilterInfo:
    def __init__(self, col_index, operation, value, col_name, col_type):
        self.col_index = col_index
        self.operation = operation
        self.value = value
        self.col_name = col_name
        self.col_type = col_type
        if col_type == "object":
            self.value = "'" + self.value + "'"

    def to_operation_tuple(self):
        operation_str = ''
        if self.operation == apsw.SQLITE_INDEX_CONSTRAINT_EQ:
            operation_str = '=='
        elif self.operation == apsw.SQLITE_INDEX_CONSTRAINT_GT:
            operation_str = '>'
        elif self.operation == apsw.SQLITE_INDEX_CONSTRAINT_LT:
            operation_str = '<'
        return (self.col_name, operation_str, self.value)


class PandasCursor:


    def __init__(self, table):
        self.table = table
        self.sort_data_frame = False
        self.pd_data_frame = self.table.pd_data_frame
        self.filter_data_frame = self.pd_data_frame

    def Filter(self, indexnum, filter_json, constraintargs):
        filter_list = []
        if filter_json != None:
            col_names = self.pd_data_frame.columns.tolist()
            col_types = self.pd_data_frame.dtypes.tolist()
            filter_infomation = json.loads(filter_json)
            for i in range(len(filter_infomation)):
                filter_list.append(FilterInfo(filter_infomation[i]["col_index"], filter_infomation[i]["operation"], constraintargs[i], 
                    col_names[filter_infomation[i]["col_index"]], col_types[filter_infomation[i]["col_index"]]))
            if self.sort_data_frame == False:
                ascendings = []
                col_sort_names = []
                for filter_i in filter_list:
                    col_sort_names.append(filter_i.col_name)
                    if filter_i.operation == apsw.SQLITE_INDEX_CONSTRAINT_EQ:
                        ascendings.append(True)
                    elif filter_i.operation == apsw.SQLITE_INDEX_CONSTRAINT_GT:
                        ascendings.append(False)
                    elif filter_i.operation == apsw.SQLITE_INDEX_CONSTRAINT_LT:
                        ascendings.append(True)

                self.pd_data_frame = self.pd_data_frame.sort_values(by=col_sort_names, ascending = ascendings)
                self.sort_data_frame = True
            filter_operation_tuples = []
            for filter_i in  filter_list:
                filter_operation_tuples.append(filter_i.to_operation_tuple())
            query = ' & '.join(['{}{}{}'.format(k,operation, v) for k,operation, v in filter_operation_tuples])
            self.filter_data_frame = self.pd_data_frame.query(query)
            self.filter_data_frame.sort_index(inplace=True)
            self.pos = 0
        else:
            self.pos = 0


    def Eof(self):
        return self.pos >= len(self.filter_data_frame.values)

    def Rowid(self):
        return self.pos

    def Column(self, col):
        if self.filter_data_frame.dtypes[col] == "object":
           return str(self.filter_data_frame.values[self.pos][col])
        return self.filter_data_frame.values[self.pos][col]

    def Next(self):
        self.pos += 1
        return

    def Close(self):
        pass
