import pandas as pd
import os

class SasSourceInfo(QewsSourceInfo):
    def __init__(self, file_path):
        self.file_path = file_path

    def get_table_list(self):
        base = os.path.basename(self.file_path)
        return [os.path.splitext(base)[0]]

class SasTable(QewsParserTable):
    def __init__(self, table_name, source_info):
        if source_info == None:
            raise Exception("the parquet source info can not none")
        if table_name not in source_info.get_table_list():
            raise Exception("table name does not exist")
        QewsParserTable.__init__(self, table_name, source_info)

    def get_col_names(self):
        return pd.read_sas(self.file_path, chunksize = 0).columns.tolist()

    def parse_data_by_col_names(self, col_names):
        data_frame = pd.read_sas(self.source_info.file_path)
        return pd.DataFrame(data_frame, columns = col_names)