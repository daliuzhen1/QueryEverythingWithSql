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

    @abstractmethod
    def get_table_list(self):
        pass

    @abstractmethod
    def get_column_info_list_by_table_name(self, table_name):
        pass


class SQLParserTable():
    def __init__(self, source_info, select_field_list = None):
        pass

    @abstractmethod
    def GetColumnNames(self):
        pass

    @abstractmethod
    def BestIndex(self, constraints, orderbys):
        pass

    @abstractmethod
    def Open(self):
        pass

    @abstractmethod
    def Disconnect(self):
        pass

    
    def Commit(self):
        pass

    @abstractmethod
    def Destroy(self):
        pass

    def FindFunction(self, name, nargs):
        pass

    def Rename(self, newname):
        raise Exception("Not support rename")

    def Rollback(self):
        pass

    def Sync(self):
        pass

    def UpdateChangeRow(self):
        raise Exception("Not support updateChangeRow")

    def UpdateDeleteRow(self):
        raise Exception("Not support updateDeleteRow")        

    def UpdateInsertRow(self):
        raise Exception("Not support updateInsertRow")       

class SQLParserCursor:
    def __init__(self, table):
        pass
    @abstractmethod
    def Filter(self, indexnum, filter_json, constraintargs):
        pass

    @abstractmethod
    def Eof(self):
        pass

    @abstractmethod
    def Rowid(self):
        pass

    @abstractmethod
    def Column(self, col):
        pass

    @abstractmethod
    def Next(self):
        pass

    @abstractmethod
    def Close(self):
        pass