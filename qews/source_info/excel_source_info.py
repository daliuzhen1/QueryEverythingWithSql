from qews import *
import pandas as pd


class ExcelSourceInfo(QewsSourceInfo):
    def __init__(self, file_path):
        self.file_path = file_path

    def get_table_list(self):
        xl = pd.ExcelFile(self.file_path)
        return xl.sheet_names
    
    def get_col_names_by_table_name(self, table_name):
        xl = pd.ExcelFile(self.file_path)
        return xl.parse(sheet_name = table_name, nrows = 0).columns.tolist()

    def extract_data(self, table_name, col_names):
        xl = pd.ExcelFile(self.file_path)
        use_cols_int = []
        column_list = self.get_col_names_by_table_name(table_name)
        if col_names != None:
            for index,col_name in enumerate(col_names):
                if col_name not in column_list:
                    raise Exception(select_field + " not exist")
                else:
                    use_cols_int.append(column_list.index(col_name))
        return xl.parse(sheet_name = table_name, usecols = use_cols_int)
# class ExcelTable(QewsParserTable):
#     def __init__(self, table_name, source_info):
#         if source_info == None:
#             raise Exception("the excel source info can not none")
        
#         if table_name not in source_info.get_table_list():
#                 raise Exception("sheet " + table_name + "does not exsit")
#         QewsParserTable.__init__(self, table_name, source_info)

#     def get_col_names(self):
#         xl = pd.ExcelFile(self.source_info.file_path)
#         return xl.parse(sheet_name = self.table_name, nrows = 0).columns.tolist()

#     def parse_data_by_col_names(self, col_names):
#         xl = pd.ExcelFile(self.source_info.file_path)
#         use_cols_int = []
#         column_list = self.get_col_names()
#         if col_names != None:
#             for index,col_name in enumerate(col_names):
#                 if col_name not in column_list:
#                     raise Exception(select_field + " not exist")
#                 else:
#                     use_cols_int.append(column_list.index(col_name))
#         return xl.parse(sheet_name = self.table_name, usecols = use_cols_int)