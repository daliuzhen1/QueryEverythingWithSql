from sql_parser.pandas_sql_parser import *
import pandas as pd

class SasSourceInfo(SQLParserSourceInfo):
    def __init__(self, file_path):
        self.file_path = file_path

class SasTable(PandasTable):
    def __init__(self, sas_source_info, select_field_list = None):
        data_frame = pd.read_sas(sas_source_info.file_path)
        pd_source_info = PandasSourceInfo(data_frame)
        PandasTable.__init__(self, pd_source_info, select_field_list)