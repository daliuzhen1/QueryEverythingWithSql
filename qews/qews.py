import pandas as pd
import apsw
from abc import abstractmethod
from pandasql import sqldf
import copy
import sqlparse
import re
from multiprocessing import Pool

# from sqlparse.sql import IdentifierList, Identifier
# from sqlparse.tokens import Keyword, DML
# class ColumnNotExistException(Exception):
#     def __init__(self, table_name):
#         self.table_name = table_name
"""
get the column name which you select
"""

class _SqliteTableTracer():
    def __init__(self, table_name, qews_parser_table):
        self.table_name = table_name
        self.column_list = []
        self.qews_parser_table = qews_parser_table

    def append_col(self, col_name):
        self.column_list.append(col_name)

    def get_col_list(self):
        return self.column_list

    def parse_data(self):
        return self.qews_parser_table.parse_data_by_col_names(self.column_list)

class _SqliteTracer():
    def __init__(self, sql, extract_table_list, regist_table_dict):
        self.sqlite_table_tracer_dict = {}
        self.select_regist_table_dict = {}
        apsw_connection = apsw.Connection(":memory:")
        apsw_cursor = apsw_connection.cursor()

        for extract_table_name in extract_table_list:
            if extract_table_name not in regist_table_dict.keys():
                print (extract_table_name)
                raise Exception("table not regist")
            else:
                qews_parser_table = regist_table_dict[extract_table_name]
                statement = "create table " + extract_table_name + "("+','.join(["'%s'" % (x,) for x in qews_parser_table.get_col_names()])+")"
                apsw_cursor.execute(statement)
                self.select_regist_table_dict[extract_table_name] = qews_parser_table

        apsw_connection.setauthorizer(self._authorizer)
        # apsw_connection.setexectrace(self._exec_tracer) 
        apsw_cursor.execute(sql)

    def _authorizer(self, operation, table_name, col_name, databasename, triggerorview):
        if operation != apsw.SQLITE_READ:
            return apsw.SQLITE_OK
        if operation == apsw.SQLITE_READ:
            table_tracer = None
            if table_name not in self.sqlite_table_tracer_dict.keys():
                table_tracer = _SqliteTableTracer(table_name, self.select_regist_table_dict[table_name])
                self.sqlite_table_tracer_dict[table_name] = table_tracer
            else:
                table_tracer = self.sqlite_table_tracer_dict[table_name]
            if col_name not in table_tracer.get_col_list():
                    table_tracer.append_col(col_name)
        return apsw.SQLITE_OK

    def get_sqlite_tracer_table_dict(self):
        for table_name in self.sqlite_table_tracer_dict:
            print (table_name, self.sqlite_table_tracer_dict[table_name].column_list)
        return self.sqlite_table_tracer_dict

class SQLPaser():
    # def _is_subselect(self, parsed):
    #     if not parsed.is_group:
    #         return False
    #     print ("_is_subselect")
    #     if isinstance(parsed, Identifier):
    #         print (111111111111)
    #         print (parsed.token_first())
    #     for item in parsed.tokens:
    #         print (item.value, item.ttype)
    #         if item.ttype is DML and item.value.upper() == 'SELECT':
    #             return True
    #     print ("_is_subselect end")
    #     return False

    # def _extract_from_part(self, parsed):
    #     from_or_join_seen = False
    #     for item in parsed.tokens:
    #         if from_or_join_seen:
    #             print (item.value)
    #             if self._is_subselect(item):
    #                 for x in self._extract_from_part(item):
    #                     yield x
    #             elif item.ttype is Keyword:
    #                 if item.value.upper() != 'FROM' and item.value.upper() != 'JOIN':
    #                     from_or_join_seen = False
    #                 # raise StopIteration
    #             else:
    #                 yield item
    #         elif item.ttype is Keyword and (item.value.upper() == 'FROM' or item.value.upper() == 'JOIN'):
    #             from_or_join_seen = True

    # # def _extract_from_part(self,parsed):
    # #     from_seen = False
    # #     for item in parsed.tokens:
    # #         if from_seen:
    # #             if self._is_subselect(item):
    # #                 for x in self._extract_from_part(item):
    # #                     yield x
    # #             elif item.ttype is Keyword:
    # #                 raise StopIteration
    # #             else:
    # #                 yield item
    # #         elif item.ttype is Keyword and item.value.upper() == 'FROM':
    # #             from_seen = True

    # def _extract_table_identifiers(self, token_stream): 
    #     for item in token_stream:
    #         if isinstance(item, IdentifierList):
    #             for identifier in item.get_identifiers():
    #                 yield identifier.get_real_name()
    #         elif isinstance(item, Identifier):
    #             yield item.get_real_name()
    #         # It's a bug to check for Keyword here, but in the example
    #         # above some tables names are identified as keywords...
    #         elif item.ttype is Keyword:
    #             yield item.value

    # def _extract_tables(self, sql):
    #     stream = self._extract_from_part(sqlparse.parse(sql)[0])
    #     return list(self._extract_table_identifiers(stream))
    def _extract_tables(self, sql_str):

        # remove the /* */ comments
        q = re.sub(r"/\*[^*]*\*+(?:[^*/][^*]*\*+)*/", "", sql_str)

        # remove whole line -- and # comments
        lines = [line for line in q.splitlines() if not re.match("^\s*(--|#)", line)]

        # remove trailing -- and # comments
        q = " ".join([re.split("--|#", line)[0] for line in lines])

        # split on blanks, parens and semicolons
        tokens = re.split(r"[\s)(;]+", q)

        # scan the tokens. if we see a FROM or JOIN, we set the get_next
        # flag, and grab the next one (unless it's SELECT).

        result = set()
        get_next = False
        for tok in tokens:
            if get_next:
                if tok.lower() not in ["", "select"]:
                    result.add(tok)
                get_next = False
            get_next = tok.lower() in ["from", "join"]

        return result

    def parse(self, sql, regist_table_dict):
        extract_table_list = self._extract_tables(sql)
        print (extract_table_list)
        sqlite_tracer = _SqliteTracer(sql, extract_table_list, regist_table_dict)
        return sqlite_tracer.get_sqlite_tracer_table_dict()
"""
Qews use apsw to parse sql. get the column name which you select
"""
class Qews():
    def __init__(self):
        self.table_dict = {}
        
    """
    before query the data, you should create a table that map to your source info.
    table_name: Used by the query.
    qews_parser_table: your sql parser table
    """
    def registeTable(self, table_name, qews_parser_table):
        if table_name in self.table_dict:
            raise Exception(table_name + " has already created ")
        self.table_dict[table_name] = qews_parser_table
        return   

    def execute(self, statement):
        sql_parser = SQLPaser()
        self.env_list = {}
        sqlite_table_tracer_dict = sql_parser.parse(statement, self.table_dict)
        pd_data_dict = {}
        for table_tracer in sqlite_table_tracer_dict:
            pd_data_dict[sqlite_table_tracer_dict[table_tracer].table_name] = sqlite_table_tracer_dict[table_tracer].parse_data()
        return sqldf(statement, pd_data_dict)

    """
    use execute to run your query
    """
    def execute_multiprocess(self, statement):
        sql_parser = SQLPaser()
        self.env_list = {}
        sqlite_table_tracer_dict = sql_parser.parse(statement, self.table_dict)
        pd_data_dict = {}
        pool = Pool(processes=4)
        results = {}
        for table_tracer in sqlite_table_tracer_dict:
            results[sqlite_table_tracer_dict[table_tracer].table_name] = pool.apply_async(sqlite_table_tracer_dict[table_tracer].parse_data, ())
        for table_name in results:
            pd_data_dict[table_name] = results[table_name].get(timeout = None)
        return sqldf(statement, pd_data_dict)

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

    def get_table_name(self):
        return self.table_name

    @abstractmethod
    def parse_data_by_col_names(self, col_names):
        pass