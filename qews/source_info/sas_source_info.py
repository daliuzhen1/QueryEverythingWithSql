from qews import *
import pandas as pd
import os

class SasSourceInfo(QewsSourceInfo):
    def __init__(self, file_path):
        self.file_path = file_path

    def get_table_list(self):
        base = os.path.basename(self.file_path)
        return [os.path.splitext(base)[0]]

    def get_col_names_by_table_name(self, table_name):
        return pd.read_sas(self.file_path, chunksize = 0).columns.tolist()

    def extract_data(self, table_name, col_names):
        data_frame = pd.read_sas(self.file_path)
        return pd.DataFrame(data_frame, columns = col_names)