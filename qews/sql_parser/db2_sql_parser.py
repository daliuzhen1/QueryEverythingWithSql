import pandas as pd
import apsw
import ibm_db
import ibm_db_dbi
from sql_parser.pandas_sql_parser import *

class DB2SourceInfo:
    def __init__(self, ibm_db_connect, database_name = None, host_name = None, port = None, protocol = None, user_name = None, password = None):
        if ibm_db_connect:
            pass
        else:
            self.database_name = database_name
            self.host_name = host_name
            self.port = port
            self.protocol = protocol
            self.user_name = user_name
            self.password = password
            connect_str = ''
            if database_name:
                connect_str += ('DATABASE=' + database_name + ';')
            if host_name:
                connect_str += ('HOSTNAME=' + host_name + ';')
            if port:
                connect_str += ('PORT=' + port + ';')       
            if protocol:
                connect_str += ('PROTOCOL=' + protocol + ';')     
            if user_name:
                connect_str += ('UID=' + user_name + ';')      
            if password:
                connect_str += ('PWD=' + password + ';')                                
            ibm_db_connect = ibm_db.connect(connect_str, "", "")

        if not ibm_db_connect:
            raise Exception ("can not connect the db2")
        self.connection_ibm = ibm_db_dbi.Connection(ibm_db_connect)
        self.table_list = []
        tables = self.connection_ibm.tables()
        for table in tables:
            self.table_list.append(table['TABLE_NAME'])

    def get_table_list(self):
        return self.table_list

class DB2Table(PandasTable):
    def __init__(self, db2_source_info, table_name, select_field_list = None):
        table_name = table_name.upper()
        if table_name not in db2_source_info.get_table_list():
            raise Exception(table_name + " not exist")
        self.table_name = table_name
        self.db2_source_info = db2_source_info
        self.select_field_list = select_field_list

        columns = self.db2_source_info.connection_ibm.columns(None, self.table_name, None)
        columns_list = []
        for column in columns:
            columns_list.append(column['COLUMN_NAME'])
        
        schema_column_list = []
        if self.select_field_list != None:
            for select_field in self.select_field_list:
                if select_field.upper() not in columns_list:
                    raise Exception(select_field + " not exist")
                else:
                    schema_column_list.append(select_field)
        if len(schema_column_list) == 0:
            col_str_join = "*"
        else:
            col_str_join = ','.join(schema_column_list)
        self.data_frame = pd.read_sql("select " + col_str_join + " from " +  self.table_name, 
            self.db2_source_info.connection_ibm)
        pd_source_info = PandasSourceInfo(self.data_frame)
        PandasTable.__init__(self, pd_source_info)

    # def GetColumnNames(self):
    #     return self.schema_column_list

    # def BestIndex(self, constraints, orderbys):
    #     return None
    #     # if len(constraints) == 0:
    #     #     return None
    #     # fillter_json_array = []
    #     # ret_constraint_used  = ()
    #     # for index in range(len(constraints)):
    #     #     ret_constraint_used = ret_constraint_used + (index,)
    #     #     fillter_json = {"col_index":constraints[index][0], "operation" : constraints[index][1]}
    #     #     fillter_json_array.append(fillter_json)
    #     # fillter_json_array = json.dumps(fillter_json_array)
    #     # return ret_constraint_used, 0, fillter_json_array,False,1000


    # def Open(self):
    #     return DB2Cursor(self)

    # def Disconnect(self):
    #     pass

    # Destroy=Disconnect


# class DB2Cursor:
#     def __init__(self, table):
#         self.table = table
#         self.data_frame = pd.read_sql("select * from " + table.db2_table_source_info.db2_table_name, 
#             table.db2_table_source_info.db2_connection_info.connection_ibm)

#     def Filter(self, indexnum, filter_json, constraintargs):
#         self.pos = 0

#     def Eof(self):
#         return self.pos >= len(self.data_frame.values)

#     def Rowid(self):
#         return self.pos

#     def Column(self, col):
#         # print (self.data_frame.values[self.pos][col])
#         if self.data_frame.dtypes[col] == "object" or self.data_frame.dtypes[col] == "datetime64[ns]":
#            return str(self.data_frame.values[self.pos][col])
#         # print (self.data_frame.dtypes[col])
#         return self.data_frame.values[self.pos][col]

#     def Next(self):
#         self.pos += 1
#         return

#     def Close(self):
#         pass