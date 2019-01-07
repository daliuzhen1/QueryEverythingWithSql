from qews import *
import pandas as pd
import os

class CsvSourceInfo(QewsSourceInfo):
    def __init__(self, file_path, separator = ','):
        self.file_path = file_path
        self.separator = separator

    def get_table_list(self):
        base = os.path.basename(self.file_path)
        return [os.path.splitext(base)[0]]

class CsvTable(QewsParserTable):
    def __init__(self, table_name, source_info):
        if source_info == None:
            raise Exception("the csv source info can not none")
        if table_name not in source_info.get_table_list():
            raise Exception("table name does not exist")
        QewsParserTable.__init__(self, table_name, source_info)
        self.parse_data = None

    def get_col_names(self):
        return pd.read_csv(self.source_info.file_path, self.source_info.separator, nrows = 0).columns.tolist()

    def get_table_name(self):
        return self.table_name

    def parse_data_by_col_names(self, col_names):
        return pd.read_csv(self.source_info.file_path, sep = self.source_info.separator, usecols = col_names)