import pandas as pd
import apsw
import json
from sql_parser.sql_parser import *

class PandasSourceInfo(SQLParserSourceInfo):
    def __init__(self, pandas_data_frame):
        self.data_frame = pandas_data_frame

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

class PandasTable(SQLParserTable):
    def __init__(self, pandas_source_info, select_field_list = None):

        column_list = pandas_source_info.data_frame.columns.tolist()
        if select_field_list != None:
            for select_field in select_field_list:
                if select_field not in column_list:
                    raise Exception(select_field + " not exist")

        self.pandas_source_info = pandas_source_info
        if select_field_list != None:
            self.data_frame = pd.DataFrame(self.pandas_source_info.data_frame, columns = select_field_list)
        else:
            self.data_frame = self.pandas_source_info.data_frame

    def GetColumnNames(self):
        return self.data_frame.columns.tolist()

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


class PandasCursor(SQLParserCursor):
    def __init__(self, table):
        self.table = table
        self.sort_data_frame = False
        self.pd_data_frame = self.table.data_frame
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
        if self.filter_data_frame.dtypes[col] == "object" or self.filter_data_frame.dtypes[col] == "datetime64[ns]":
           return str(self.filter_data_frame.values[self.pos][col])
        elif self.filter_data_frame.dtypes[col] == "int64" or self.filter_data_frame.dtypes[col] == "int32":
            return int(self.filter_data_frame.values[self.pos][col])
        return self.filter_data_frame.values[self.pos][col]

    def Next(self):
        self.pos += 1
        return

    def Close(self):
        pass
