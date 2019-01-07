import pandas as pd
import apsw
from abc import abstractmethod
from pandasql import sqldf
import copy

# class ColumnNotExistException(Exception):
#     def __init__(self, table_name):
#         self.table_name = table_name

class _SqliteTableTracer():
    def __init__(self, table_name, qews_parser_table):
        self.table_name = table_name
        self.column_list = []
        self.qews_parser_table = qews_parser_table

    def append_col(self, col_name):
        self.column_list.append(col_name)


    def parse_data(self):
        return self.qews_parser_table.parse_data_by_col_names(self.column_list)

class _SqliteTracer():
    def __init__(self, source_table_list):
        self.sqlite_table_tracer_list = {}
        self.pd_data_list = {}
        self.source_table_list = source_table_list
    def _authorizer(self, operation, table_name, col_name, databasename, triggerorview):
        if operation != apsw.SQLITE_READ:
            return apsw.SQLITE_OK
        # if col_name == '':
        #     raise ColumnNotExistException(table_name)
        if operation == apsw.SQLITE_READ:
            table_tracer = None
            if table_name not in self.sqlite_table_tracer_list.keys():
                table_tracer = _SqliteTableTracer(table_name, self.source_table_list[table_name])
                self.sqlite_table_tracer_list[table_name] = table_tracer
            else:
                table_tracer = self.sqlite_table_tracer_list[table_name]
            table_tracer.append_col(col_name)
        return apsw.SQLITE_OK

    def _exec_tracer(self, cursor, sql, bindings):
        for table_tracer in self.sqlite_table_tracer_list:
            self.pd_data_list[self.sqlite_table_tracer_list[table_tracer].table_name] = self.sqlite_table_tracer_list[table_tracer].parse_data()
        return True

    def _get_pd_data_list(self):
        return self.pd_data_list

"""
Qews use apsw to parse sql. get the column name which you select
"""
class Qews():
    def __init__(self):
        self.table_list = {}
        self.apsw_connection = apsw.Connection(":memory:")
        self.apsw_cursor = self.apsw_connection.cursor()
    """
    before query the data, you should create a table that map to your source info.
    table_name: Used by the query.
    qews_parser_table: your sql parser table
    """
    def registeTable(self, table_name, qews_parser_table):
        if table_name in self.table_list:
            raise Exception(table_name + " has already created ")
        self.table_list[table_name] = qews_parser_table
        statement = "create table " + table_name + "("+','.join(["'%s'" % (x,) for x in qews_parser_table.get_col_names()])+")"
        self.apsw_cursor.execute(statement)       

    """
    use execute to run your query
    """
    def execute(self, statement):
        self.env_list = {}
        """
        after run this method, firstly will call _authorizer to parse column that you need, and run _exec_tracer 
        to get data from source. Then use sqldf to select data
        """ 
        sqlite_tracer = _SqliteTracer(self.table_list) 
        self.apsw_connection.setauthorizer(sqlite_tracer._authorizer)
        self.apsw_connection.setexectrace(sqlite_tracer._exec_tracer)
        # try:
        self.apsw_cursor.execute(statement)
        # except ColumnNotExistException as e:
        #     print ("your select column does not in " + e.table_name)
        #     return None
        return sqldf(statement, sqlite_tracer._get_pd_data_list())


class QewsSourceInfo():
    def __init__(self):
        pass

    @abstractmethod
    def get_table_list(self):
        pass

class QewsParserTable():
    def __init__(self, table_name, qews_source_info):
        self.table_name = table_name
        self.source_info = qews_source_info
        self._select_col_list = []

    @abstractmethod
    def get_col_names(self):
        pass

    @abstractmethod
    def get_table_name(self):
        return self.table_name

    @abstractmethod
    def parse_data_by_col_names(self, col_names):
        pass