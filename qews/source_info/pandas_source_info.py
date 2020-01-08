from qews import *
import pandas as pd

class PandasSourceInfo(QewsSourceInfo):
    def __init__(self, data_frame):
        self.data_frame = data_frame

    def get_table_list(self):
        return None