from qews import *
import pandas as pd
import apsw

class PandasSourceInfo(QewsSourceInfo):
    def __init__(self, data_frame):
        self.data_frame = data_frame

    def get_table_list(self):
        return None
    
class PandasTable(QewsParserTable):
    def __init__(self, table_name, source_info):
        if source_info == None:
            raise Exception("the excel source info can not none")
        self.source_info = source_info
        self.table_name = table_name
        QewsParserTable.__init__(self, table_name, source_info)

    def get_col_names(self):
        return self.source_info.data_frame.columns.tolist()

    def get_table_name(self):
        return self.table_name

    def parse_data_by_col_names(self, col_names):
        return pd.DataFrame(self.pandas_source_info.data_frame, columns = col_names)