import pandas as pd
import apsw
import json
from abc import abstractmethod
"""
It is a wrapper for apsw
apsw_connection:It is a apsw connection, all of sql query based on this connection
module name: you can special a module name, or use default
"""
class SQLParserModule():
    def __init__(self, apsw_connection, module_name = "sql_parser_module"):
        self.table_list = {}
        # self.support_model_list = ["csv", "excel", "pandas", "db2"]
        # if module_type.lower() not in support_model_list:
        #     raise Exception("not support " + module_type)
        self.module_name = module_name
        if not self.module_name:
            raise Exception("you should set module name")
        try:
            apsw_connection.createmodule(self.module_name, self)
            self.apsw_connection = apsw_connection
            self.apsw_cursor = self.apsw_connection.cursor()    
        except:
            raise Exception("create module error, may be you have already create module " + self.module_name)
    """
    before query the data, you should create a table that map to your source info.
    table_name: Used by the query.
    source_info: Support csv, excel, pandas, db2 source

    """
    def registeTable(self, table_name, sql_parser_table):
        if table_name in self.table_list:
            raise Exception(table_name + " has already created ")
        self.table_list[table_name] = sql_parser_table
        self.apsw_cursor.execute("create virtual table "+ table_name +" using " + self.module_name)

    def Create(self, db, modulename, dbname, tablename, *args):
        col_names = self.table_list[tablename].GetColumnNames()
        schema="create table X("+','.join(["'%s'" % (x,) for x in col_names])+")"
        return schema, self.table_list[tablename]

    Connect = Create

class SQLParserSourceInfo():
    def __init__(self):
        pass


class SQLParserTable():
    """
    field_map: It is a map. Default is None.Key represent which column name you will be used in the query. value is map the source field
               For example, if you a have column named test in your table. and you want query it by test1. you can set 
               field_map[test1] = test.If you don't set it. It use source field name.
    """
    def __init__(self, source_info, select_field_list = None):
        pass
    """
    parse source info and set your table
    Return: column list
    """
    @abstractmethod
    def GetColumnNames(self):
        raise Exception("please implement GetColumnNames in your class")

    @abstractmethod
    def BestIndex(self, constraints, orderbys):
        raise Exception("please implement BestIndex in your class")

    @abstractmethod
    def Open(self):
        raise Exception("please implement Open in your class")

    @abstractmethod
    def Disconnect(self):
        raise Exception("please implement Disconnect in your class")


class SQLParserCursor:
    def __init__(self, table):
        pass
    @abstractmethod
    def Filter(self, indexnum, filter_json, constraintargs):
        raise Exception("please implement Filter in your class")

    @abstractmethod
    def Eof(self):
        raise Exception("please implement Eof in your class")

    @abstractmethod
    def Rowid(self):
        raise Exception("please implement Rowid in your class")

    @abstractmethod
    def Column(self, col):
        raise Exception("please implement Column in your class")

    @abstractmethod
    def Next(self):
        raise Exception("please implement Next in your class")

    @abstractmethod
    def Close(self):
        raise Exception("please implement Next in your class")