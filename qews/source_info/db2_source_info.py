from qews import *
import pandas as pd
import ibm_db_dbi

class DB2SourceInfo(QewsSourceInfo):
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
        self.connection_ibm = ibm_db_dbi.connect(PORT = port, PROTOCOL = 'TCPIP', host = host_name, database = database_name, user= user_name, password = password)
        

    def get_table_list(self):
        table_list = []
        tables = self.connection_ibm.tables()
        for table in tables:
            table_list.append(table['TABLE_NAME'])
        return table_list

    def get_col_names_by_table_name(self, table_name):
        columns = self.connection_ibm.columns(None, table_name, None)
        columns_list = []
        for column in columns:
            columns_list.append(column['COLUMN_NAME'])
        return columns_list

    def extract_data(self, table_name, col_names):
        col_str_join = ','.join(col_names)
        print (col_str_join)
        data_frame = pd.read_sql("select " + col_str_join + " from " +  table_name, 
            self.connection_ibm)
        return data_frame


# class DB2Table(QewsParserTable):
#     def __init__(self, table_name, source_info):
#         if source_info == None:
#             raise Exception("the csv source info can not none")
#         if table_name not in source_info.get_table_list():
#             raise Exception("table name does not exist")
#         QewsParserTable.__init__(self, table_name, source_info)

#     def get_col_names(self):
#         columns = self.source_info.connection_ibm.columns(None, self.table_name, None)
#         columns_list = []
#         for column in columns:
#             columns_list.append(column['COLUMN_NAME'])
#         return columns_list

#     def get_table_name(self):
#         return self.table_name

#     def parse_data_by_col_names(self, col_names):
#         schema_column_list = []
#         columns_list = self.source_info.get_col_names()
#         if col_names != None:
#             for col_name in col_names:
#                 if col_name.upper() not in columns_list:
#                     raise Exception(select_field + " not exist")
#                 else:
#                     schema_column_list.append(col_name)

#         if len(schema_column_list) == 0:
#             col_str_join = "*"
#         else:
#             col_str_join = ','.join(schema_column_list)
#         data_frame = pd.read_sql("select " + col_str_join + " from " +  self.table_name, 
#             self.source_info.connection_ibm)
#         return data_frame