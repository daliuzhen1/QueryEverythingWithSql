from sql_parser.pandas_sql_parser import *
import pandas as pd
import apsw

class CsvSourceInfo(SQLParserSourceInfo):
    def __init__(self, file_path, separator = ','):
        self.file_path = file_path
        self.separator = separator

class CsvTable(PandasTable):
    def __init__(self, csv_source_info):
        self.data_frame = pd.read_csv(csv_source_info.file_path, sep = csv_source_info.separator)
        pd_source_info = PandasSourceInfo(self.data_frame)
        PandasTable.__init__(self, pd_source_info)