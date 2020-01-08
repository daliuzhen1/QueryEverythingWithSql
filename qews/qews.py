import pandas as pd
from abc import abstractmethod
from multiprocessing import Pool
import sqlite3
from sql_core.sql_df import sqldf
from sql_core.sql_parser import SQLPaser
from sql_core.formular import Formular
from sql_core.aggregate_dash import *

"""
Qews use sqlite3 to parse sql. get the column name which you select
"""
class Qews():
    def __init__(self):
        self.table_dict = {}
        self.source_info_dict = {}

    def add_source(self, source_name, source_info):
        if source_name in self.source_info_dict:
            return False
        self.source_info_dict[source_name] = source_info
        return True

    def get_source_info(self, source_name):
        return self.source_info_dict[source_name]

    def execute(self, statement):
        sqlite3_connection = sqlite3.connect(":memory:")
        Formular.Create(sqlite3_connection)
        uuid = Dash.Create(sqlite3_connection)
        sql_parser = SQLPaser()
        env_list = {}
        sqlite_source_tracer_dict = sql_parser.parse(sqlite3_connection, statement, self.source_info_dict)
        for source_tracer in sqlite_source_tracer_dict:
            pd_data_dict = sqlite_source_tracer_dict[source_tracer].extract_data()
            for item in pd_data_dict:
                env_list[item] = pd_data_dict[item]
        df = sqldf(sqlite3_connection, statement, env_list)
        dash = Dash.get_and_pop_dash_by_uuid(uuid)
        plot_list = dash.get_plot_list()
        if len(plot_list) > 0:
            return plot_list
        else:
            return df

class QewsSourceInfo():
    def __init__(self):
        pass

    @abstractmethod
    def get_table_list(self):
        pass

    @abstractmethod
    def get_col_names_by_table_name(self, table_name):
        pass

    @abstractmethod
    def extract_data(self, table_name, col_names):
        pass
