import pandas as pd
import apsw
class ExcelSourceInfo:
    def __init__(self, file_path):
            self.file_path = file_path
            xl = pd.ExcelFile(self.file_path)
            self.sheet_names = xl.sheet_names

class ExcelRegisteTableInfo:
    def __init__(self, table_name, sheet_name, excel_source_info):
        self.table_name = table_name
        self.excel_source_info = excel_source_info
        self.sheet_name = sheet_name
        if sheet_name not in self.excel_source_info.sheet_names:
            raise Exception("sheet " + sheet_name + "does not exsit")


class ExcelModule:
    def __init__(self):
        self.table_list = {}

    def registeTable(self, excel_resgiste_table_info):
        self.table_list[excel_resgiste_table_info.table_name] = ExcelTable(excel_resgiste_table_info)

    def Create(self, db, modulename, dbname, tablename, *args):
        return self.table_list[tablename].declareTable()

    Connect = Create


class ExcelTable:
    def __init__(self, excel_resgiste_table_info):
        self.excel_resgiste_table_info = excel_resgiste_table_info
        self.pd_data_frame = None
    
    def declareTable(self):
        df = pd.read_excel(self.excel_resgiste_table_info.excel_source_info.file_path, self.excel_resgiste_table_info.sheet_name)
        schema="create table X("+','.join(["'%s'" % (x,) for x in df.columns.tolist()])+")"
        self.pd_data_frame = df
        return schema, self

    def BestIndex(self, *args):
        return None

    def Open(self):
        return ExcelCursor(self)

    def Disconnect(self):
        pass

    Destroy=Disconnect

class ExcelCursor:
    def __init__(self, table):
        self.table = table

    def Filter(self, *args):
        self.pos = 0

    def Eof(self):
        return self.pos >= len(self.table.pd_data_frame.values)

    def Rowid(self):
        return self.table.data[self.pos][0]

    def Column(self, col):
        if self.table.pd_data_frame.dtypes[col] == "object":
           return str(self.table.pd_data_frame.values[self.pos][col])
        return self.table.pd_data_frame.values[self.pos][col]

    def Next(self):
        self.pos += 1
        return

    def Close(self):
        pass
