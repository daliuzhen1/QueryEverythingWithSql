from qews import *
import pandas as pd

class PandasSourceInfo(QewsSourceInfo):
    def __init__(self, data_frame):
        self.data_frame = data_frame

    def get_table_list(self):
        return None
    
# class PandasTable(QewsParserTable):
#     def __init__(self, source_info):
#         if source_info == None:
#             raise Exception("the excel source info can not none")
#         self.source_info = source_info
#         QewsParserTable.__init__(self, None, source_info)

#     def get_col_names(self):
#         return self.source_info.data_frame.columns.tolist()

#     def parse_data_by_col_names(self, col_names):
#         return pd.DataFrame(self.source_info.data_frame, columns = col_names)