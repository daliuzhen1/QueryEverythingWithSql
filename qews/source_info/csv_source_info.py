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

    def get_col_names_by_table_name(self, table_name):
        return pd.read_csv(self.file_path, self.separator, nrows = 0).columns.tolist()

    def extract_data(self, table_name, col_names):
        return pd.read_csv(self.file_path, sep = self.separator, usecols = col_names)