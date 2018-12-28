from sql_parser.pandas_sql_parser import *
import pandas as pd
from enum import IntEnum
import apsw

class CsvTableSourceInfo:
    def __init__(self, table_name, file_path, separator = ','):
            self.file_path = file_path
            self.table_name = table_name
            self.data_frame = pd.read_csv(self.file_path, sep = separator)

class CsvModule:
    def __init__(self, connection):
        self.table_list = {}
        self.module_name = "csv"
        connection.createmodule(self.module_name, self)

    def createTable(self, cursor , csv_table_source_info):
        pandas_table_source_info = PandasTableSourceInfo(csv_table_source_info.table_name, csv_table_source_info.data_frame)
        self.table_list[csv_table_source_info.table_name] = PandasTable(pandas_table_source_info)
        cursor.execute("create virtual table "+ csv_table_source_info.table_name +" using " + self.module_name)

    def Create(self, db, modulename, dbname, tablename, *args):
        return self.table_list[tablename].declareTable()

    Connect = Create
