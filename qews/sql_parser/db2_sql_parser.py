from sql_parser.pandas_sql_parser import *
import pandas as pd
from enum import IntEnum
import apsw
from ibm_db import connect

class DB2TableSourceInfo:
    def __init__(self, table_name, table_name_db2, database_name, host_name, port, protocol, user_name, password):
        self.database_name = database_name
        self.host_name = host_name
        self.port = port
        self.protocol = protocol
        self.user_name = user_name
        self.password = password
        connect_str += ("DATABASE=" + self.database_name + ";")
        connect_str += ("HOSTNAME=" + self.host_name + ";")
        connect_str += ("PORT=" + self.port + ";")
        connect_str += ("PROTOCOL=" + self.protocol + ";")
        connect_str += ("UID=" + self.user_name + ";")
        connect_str += ("PWD=" + self.UID + ";")
        print (ibm_db.columns)
        self.connection_ibm = connect(connect_str, "", "")

class DB2Module:
    def __init__(self, connection_apsw):
        self.table_list = {}
        self.module_name = "db2"
        connection_apsw.createmodule(self.module_name, self)

    def createTable(self, cursor , db2_table_source_info):
        pandas_table_source_info = PandasTableSourceInfo(csv_table_source_info.table_name, csv_table_source_info.data_frame)
        self.table_list[csv_table_source_info.table_name] = PandasTable(pandas_table_source_info)
        cursor.execute("create virtual table "+ csv_table_source_info.table_name +" using " + self.module_name)

    def Create(self, db, modulename, dbname, tablename, *args):
        return self.table_list[tablename].declareTable()

    Connect = Create
