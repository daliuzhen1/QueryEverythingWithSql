from sql_parser.csv_sql_parser import *
from sql_parser.excel_sql_parser import *
from sql_parser.pandas_sql_parser import *
import pandas as pd
def main():
    csv_source_info = CsvSourceInfo("C:\\Users\\zhenl\\Documents\\yahoo_prices.csv")
    connection=apsw.Connection(":memory:")
    csvModule = CsvModule()
    csv_resgiste_table_info  = CsvRegisteTableInfo("yahoo_prices" , csv_source_info)
    csvModule.registeTable(csv_resgiste_table_info)
    connection.createmodule("csv", csvModule)
    cursor=connection.cursor()
    cursor.execute("create virtual table yahoo_prices using csv")

    excel_source_info = ExcelSourceInfo("C:\\Users\\zhenl\\Desktop\\Discover_Sales_THINK.xlsx")
    excel_resgiste_table_info = ExcelRegisteTableInfo("Discover_Sales_THINK", "Discover_Sales_THINK", excel_source_info)
    excel_Module = ExcelModule()
    excel_Module.registeTable(excel_resgiste_table_info)
    # print (xl.sheet_names)
    connection.createmodule("excel", excel_Module)
    cursor=connection.cursor()
    cursor.execute("create virtual table Discover_Sales_THINK using excel")

    df = pd.read_csv("C:\\Users\\zhenl\\Documents\\yahoo_prices.csv")
    psi = PandasSourceInfo(df)
    prti = PandasRegisteTableInfo("yahoo_prices_pandas", psi)
    pandasModel = PandasModule()
    pandasModel.registeTable(prti)
    connection.createmodule("pandas", pandasModel)
    cursor.execute("create virtual table yahoo_prices_pandas using pandas")

    data = cursor.execute("select * from yahoo_prices_pandas")
    print (data)
    for i in data:
        print (i)
    
if __name__ == '__main__':
    main()