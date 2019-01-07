from qews import *
import pandas as pd


class ExcelSourceInfo(QewsSourceInfo):
    def __init__(self, file_path):
        self.file_path = file_path
        self.xl = pd.ExcelFile(self.file_path)

    def get_table_list(self):
        return self.xl.sheet_names
    

class ExcelTable(QewsParserTable):
    def __init__(self, table_name, source_info):
        if source_info == None:
            raise Exception("the excel source info can not none")
        
        if table_name not in source_info.get_table_list():
                raise Exception("sheet " + table_name + "does not exsit")
        QewsParserTable.__init__(self, table_name, source_info)

    def get_col_names(self):
        return self.source_info.xl.parse(sheet_name = self.table_name, nrows = 0).columns.tolist()

    def get_table_name(self):
        return self.table_name

    def parse_data_by_col_names(self, col_names):
        use_cols_int = []
        column_list = self.get_col_names()
        if col_names != None:
            for index,col_name in enumerate(col_names):
                if col_name not in column_list:
                    raise Exception(select_field + " not exist")
                else:
                    use_cols_int.append(column_list.index(col_name))
        return self.source_info.xl.parse(sheet_name = self.table_name, usecols = use_cols_int)