from qews import *
import os
import pyarrow.parquet as pq

class ParquetSourceInfo(QewsSourceInfo):
    def __init__(self, file_path):
        self.file_path = file_path

    def get_table_list(self):
        base = os.path.basename(self.file_path)
        return [os.path.splitext(base)[0]]

class ParquetTable(QewsParserTable):
    def __init__(self, table_name, source_info):
        if source_info == None:
            raise Exception("the parquet source info can not none")
        if table_name not in source_info.get_table_list():
            raise Exception("table name does not exist")
        QewsParserTable.__init__(self, table_name, source_info)

    def get_col_names(self):
        parquet_file = pq.ParquetFile(self.source_info.file_path)
        return parquet_file.schema.names

    def get_table_name(self):
        return self.table_name

    def parse_data_by_col_names(self, col_names):
        return pq.read_pandas(self.source_info.file_path, col_names).to_pandas()