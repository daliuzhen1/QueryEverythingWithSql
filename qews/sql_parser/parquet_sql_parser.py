from sql_parser.pandas_sql_parser import *
import pandas as pd

class ParquetSourceInfo(SQLParserSourceInfo):
    def __init__(self, file_path):
        self.file_path = file_path

class ParquetTable(PandasTable):
    def __init__(self, parquet_source_info, select_field_list = None):
        data_frame = pd.read_parquet(parquet_source_info.file_path, columns = select_field_list)
        pd_source_info = PandasSourceInfo(data_frame)
        PandasTable.__init__(self, pd_source_info, select_field_list)