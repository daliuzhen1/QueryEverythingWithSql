from sql_parser.pandas_sql_parser import *
import pandas as pd
import os

class CsvSourceInfo(SQLParserSourceInfo):
    def __init__(self, file_path, separator = ','):
        self.file_path = file_path
        self.separator = separator

    def get_table_list(self):
        base = os.path.basename(self.file_path)
        return [os.path.splitext(base)[0]]

    def get_column_info_list_by_table_name(self, table_name):
        return pd.read_csv(self.file_path, self.separator, nrows = 0).columns.tolist()

class CsvTable(PandasTable):
    def __init__(self, csv_source_info, select_field_list = None):
        self.data_frame = pd.read_csv(csv_source_info.file_path, sep = csv_source_info.separator, usecols = select_field_list)
        pd_source_info = PandasSourceInfo(self.data_frame)
        PandasTable.__init__(self, pd_source_info)