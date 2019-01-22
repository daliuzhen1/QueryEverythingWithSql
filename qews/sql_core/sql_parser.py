"""
get the column name which you select
"""
import sqlite3
import re

source_splite = '_'

class _SqliteSourceTracer():
    def __init__(self, source_name, source_info):
        self.source_name = source_name
        self.source_info = source_info
        self.sql_table_tracer_list = []
    def append_table(self, table_name):
        sqlite_table_tracer = _SqliteTableTracer(table_name)
        self.sql_table_tracer_list.append(sqlite_table_tracer)
    def get_table_tracer_list(self):
        return self.sql_table_tracer_list
    def get_source_info(self):
        return self.source_info
    def extract_data(self):
        pd_frame_dict = {}
        for table_tracer in self.sql_table_tracer_list:
            str_list = table_tracer.get_table_name().split(source_splite, 1)
            real_table_name = str_list[1]
            pd_frame_dict[table_tracer.get_table_name()] = self.source_info.extract_data(real_table_name, table_tracer.get_col_list())
        return pd_frame_dict


class _SqliteTableTracer():
    def __init__(self, table_name):
        self.table_name = table_name
        self.column_list = []

    def append_col(self, col_name):
        self.column_list.append(col_name)

    def get_col_list(self):
        return self.column_list

    def get_table_name(self):
        return self.table_name

class _SqliteTracer():
    def __init__(self, sqlite3_connection, sql, sqlite_source_tracer_dict):
        sqlite3_cursor = sqlite3_connection.cursor()
        self.sqlite_source_tracer_dict = sqlite_source_tracer_dict
        for source_name in sqlite_source_tracer_dict:
            sql_table_tracer_list = sqlite_source_tracer_dict[source_name].get_table_tracer_list()
            for table_tracer in sql_table_tracer_list:
                source_info = sqlite_source_tracer_dict[source_name].get_source_info()
                str_list = table_tracer.get_table_name().split(source_splite, 1)
                real_table_name = str_list[1]
                statement = "create table " + table_tracer.get_table_name() + "("+','.join(["'%s'" % (x,) for x in source_info.get_col_names_by_table_name(real_table_name)])+")"
                print (statement)
                sqlite3_cursor.execute(statement)

        sqlite3_connection.set_authorizer(self._authorizer)
        sqlite3_cursor.execute(sql)
        sqlite3_connection.set_authorizer(self._rollback_authorizer)

        for source_name in sqlite_source_tracer_dict:
            sql_table_tracer_list = sqlite_source_tracer_dict[source_name].get_table_tracer_list()
            for table_tracer in sql_table_tracer_list:
                statement = "drop table " + table_tracer.get_table_name()
                sqlite3_cursor.execute(statement)

    def _rollback_authorizer(self, operation, table_name, col_name, databasename, triggerorview):
        return sqlite3.SQLITE_OK

    def _authorizer(self, operation, table_name, col_name, databasename, triggerorview):
        if operation != sqlite3.SQLITE_READ:
            return sqlite3.SQLITE_OK
        if operation == sqlite3.SQLITE_READ:
            table_tracer = None
            str_list = table_name.split(source_splite, 1)
            source_name = str_list[0]
            table_tracer_list = self.sqlite_source_tracer_dict[source_name].get_table_tracer_list()
            for table_tracer in table_tracer_list:
                if table_tracer.get_table_name() == table_name:
                    table_tracer.append_col(col_name)
        return sqlite3.SQLITE_OK

    def get_sqlite_source_tracer_dict(self):
        return self.sqlite_source_tracer_dict

class SQLPaser():
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

    def parse(self, sqlite3_connection, sql, source_info_dict):
        sqlite_source_tracer_dict = {}
        extract_table_list = self._extract_tables(sql)
        for extract_table in extract_table_list:
            str_list = extract_table.split(source_splite, 1)
            source_name = str_list[0]
            table_name = str_list[1]
            if source_name not in source_info_dict:
                raise Exception(source_name + ' does not exist')
            if table_name not in source_info_dict[source_name].get_table_list():
                raise Exception(table_name + ' does not exist in ' + source_name)

            if source_name in sqlite_source_tracer_dict:
                sqlite_source_tracer_dict[source_name].append_table(extract_table)
            else:
                sqlite_source_tracer_dict[source_name] = _SqliteSourceTracer(source_name, source_info_dict[source_name])
                sqlite_source_tracer_dict[source_name].append_table(extract_table)
        

        sqlite_tracer = _SqliteTracer(sqlite3_connection, sql, sqlite_source_tracer_dict)
        return sqlite_tracer.get_sqlite_source_tracer_dict()