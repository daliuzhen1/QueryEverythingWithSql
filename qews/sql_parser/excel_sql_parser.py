from sql_parser.pandas_sql_parser import *
import pandas as pd

class ExcelSourceInfo(SQLParserSourceInfo):
    def __init__(self, file_path):
            self.file_path = file_path
            self.xl = pd.ExcelFile(self.file_path)

    def get_table_list(self):
        return self.xl.sheet_names

    def get_column_info_list_by_table_name(self, table_name):
        return pd.read_excel(self.file_path, sheet_name = table_name, nrows = 0).columns.tolist()
    

class ExcelTable(PandasTable):
    def __init__(self, excel_source_info, table_name, select_field_list = None):
        if table_name not in excel_source_info.get_table_list():
                raise Exception("sheet " + table_name + "does not exsit")
        column_list = excel_source_info.get_column_info_list_by_table_name(table_name)
        use_cols_int = []
        if select_field_list != None:
            for index,select_field in enumerate(select_field_list):
                if select_field not in column_list:
                    raise Exception(select_field + " not exist")
                else:
                    use_cols_int.append(column_list.index(select_field))
        data_frame = pd.read_excel(excel_source_info.file_path, sheet_name = table_name, usecols = use_cols_int) 
        pd_source_info = PandasSourceInfo(data_frame)
        PandasTable.__init__(self, pd_source_info)