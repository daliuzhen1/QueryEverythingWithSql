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