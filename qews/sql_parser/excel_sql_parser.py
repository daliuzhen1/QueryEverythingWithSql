from sql_parser.pandas_sql_parser import *
import pandas as pd
import apsw

class ExcelSourceInfo(SQLParserSourceInfo):
    def __init__(self, file_path):
            self.file_path = file_path
            self.xl = pd.ExcelFile(self.file_path)
    def get_sheet_list(self):
        return self.xl.sheet_names
    

class ExcelTable(PandasTable):
    def __init__(self, excel_source_info, sheet_name):
        if sheet_name not in excel_source_info.get_sheet_list():
                raise Exception("sheet " + sheet_name + "does not exsit")    
        data_frame = pd.read_excel(excel_source_info.file_path, sheet_name)    
        pd_source_info = PandasSourceInfo(data_frame)
        PandasTable.__init__(self, pd_source_info)