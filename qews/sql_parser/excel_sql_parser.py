from sql_parser.pandas_sql_parser import *
import pandas as pd

class ExcelSourceInfo(SQLParserSourceInfo):
    def __init__(self, file_path):
            self.file_path = file_path
            self.xl = pd.ExcelFile(self.file_path)
    def get_sheet_list(self):
        return self.xl.sheet_names
    

class ExcelTable(PandasTable):
    def __init__(self, excel_source_info, sheet_name, select_field_list = None):
        if sheet_name not in excel_source_info.get_sheet_list():
                raise Exception("sheet " + sheet_name + "does not exsit")
        data_frame = pd.read_excel(excel_source_info.file_path, sheet_name = sheet_name, nrows = 0) 
        column_list = data_frame.columns.tolist()
        use_cols_int = []
        if select_field_list != None:
            for index,select_field in enumerate(select_field_list):
                if select_field not in column_list:
                    raise Exception(select_field + " not exist")
                else:
                    use_cols_int.append(column_list.index(select_field))
        data_frame = pd.read_excel(excel_source_info.file_path, sheet_name = sheet_name, usecols = use_cols_int) 
        pd_source_info = PandasSourceInfo(data_frame)
        PandasTable.__init__(self, pd_source_info)