from sql_parser.csv_sql_parser import *
from sql_parser.excel_sql_parser import *
from sql_parser.pandas_sql_parser import *
import ibm_db
import pandas as pd

def main():
    # csv_source_info = CsvSourceInfo("C:\\Users\\zhenl\\Documents\\yahoo_prices.csv")
    connection=apsw.Connection(":memory:")
    connection_ibm = ibm_db.connect('DATABASE=aldonlm;''HOSTNAME=10.15.100.103;''PORT=50006;''PROTOCOL=TCPIP;''UID=aldondbi;''PWD=zf9j3Hw;', '', '')
    table = ibm_db.tables(connection_ibm)
    r = ibm_db.fetch_both(table)
    # print (table.TABLE_SCHEMA)
    while(r):
        print (r['TABLE_NAME'])
        r = ibm_db.fetch_both(table)
    # csvModule = CsvModule()
    # csv_resgiste_table_info  = CsvRegisteTableInfo("yahoo_prices" , csv_source_info)
    # csvModule.registeTable(csv_resgiste_table_info)
    # connection.createmodule("csv", csvModule)
    cursor = connection.cursor()
    # cursor.execute("create virtual table yahoo_prices using csv")

    # excel_source_info = ExcelSourceInfo("C:\\Users\\zhenl\\Desktop\\Discover_Sales_THINK.xlsx")
    # excel_resgiste_table_info = ExcelRegisteTableInfo("Discover_Sales_THINK", "Discover_Sales_THINK", excel_source_info)
    # excel_Module = ExcelModule()
    # excel_Module.registeTable(excel_resgiste_table_info)
    # # print (xl.sheet_names)
    # connection.createmodule("excel", excel_Module)
    # cursor=connection.cursor()
    # cursor.execute("create virtual table Discover_Sales_THINK using excel")

    # df = pd.read_csv("C:\\Users\\zhenl\\Documents\\yahoo_prices.csv")
    # pandasModel = CsvModule("pandas")

    # ptsi = PandasTableSourceInfo("yahoo_prices", df)
    # connection.createmodule(pandasModel.module_name, pandasModel)
    # pandasModel.createTable(cursor, ptsi)

    # data = cursor.execute("select Country, max(Volume) from yahoo_prices where (Country = \'USA\' or Country = \'aaa\') and AdjClose > 25.63 and Volume > 40125200 group by Country")
    # for i in data:
    #     print (i)
    csvModule = CsvModule(connection)

    ctsi = CsvTableSourceInfo("csv_yahoo_prices1", "C:\\Users\\zhenl\\Documents\\yahoo_prices.csv")    
    csvModule.createTable(cursor, ctsi)
    data = cursor.execute("select Country, max(Volume) from csv_yahoo_prices1 where (Country = \'USA\' or Country = \'aaa\') and AdjClose > 25.63 and Volume > 40125200 group by Country")
    for i in data:
        print (i)

if __name__ == '__main__':
    main()