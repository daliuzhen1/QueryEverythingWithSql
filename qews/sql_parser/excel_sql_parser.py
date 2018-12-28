from sql_parser.pandas_sql_parser import *
import pandas as pd
import apsw

class ExcelTableSourceInfo:
    def __init__(self, table_name, sheet_name, file_path):
            self.file_path = file_path
            self.table_name = table_name
            xl = pd.ExcelFile(self.file_path)
            if sheet_name not in xl.sheet_names:
                raise Exception("sheet " + sheet_name + "does not exsit")           
            self.data_frame = pd.read_excel(self.file_path, sheet_name)

class ExcelModule:
    def __init__(self, connection):
        self.table_list = {}
        self.module_name = "excel"
        connection.createmodule(self.module_name, self)

    def createTable(self, cursor , excel_table_source_info):
        pandas_table_source_info = PandasTableSourceInfo(excel_table_source_info.table_name, excel_table_source_info.data_frame)
        self.table_list[excel_table_source_info.table_name] = PandasTable(pandas_table_source_info)
        cursor.execute("create virtual table "+ excel_table_source_info.table_name +" using " + self.module_name)

    def Create(self, db, modulename, dbname, tablename, *args):
        return self.table_list[tablename].declareTable()

    Connect = Create