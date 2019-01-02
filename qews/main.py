from sql_parser.csv_sql_parser import *
from sql_parser.excel_sql_parser import *
from sql_parser.pandas_sql_parser import *
from sql_parser.db2_sql_parser import *
from sql_parser.sql_parser import *
import ibm_db
import pandas as pd

def main():
    # csv_source_info = CsvSourceInfo("C:\\Users\\zhenl\\Documents\\yahoo_prices.csv")
    df = pd.read_csv("C:\\Users\\zhenl\\Documents\\yahoo_prices.csv")
    connection=apsw.Connection(":memory:")
    sql_parser_module = SQLParserModule(connection, "sql_parser_module")
    pd_source_info = PandasSourceInfo(df)
    pd_table = PandasTable(pd_source_info)
    sql_parser_module.createTable("yahoo_prices", pd_table)
    cursor = connection.cursor()
    # data = cursor.execute("select * from yahoo_prices where Country = \'aaa\'")
    # for i in data:
    #     print (i)
    csv_source_info = CsvSourceInfo("C:\\Users\\zhenl\\Documents\\yahoo_prices.csv")
    csv_table = CsvTable(csv_source_info)
    sql_parser_module.createTable("csv_yahoo_prices", csv_table)
    # data = cursor.execute("select * from csv_yahoo_prices where Country = \'aaa\'")
    # for i in data:
    #     print (i)

    excel_source_info = ExcelSourceInfo("C:\\Users\\zhenl\\Desktop\\Discover_Sales_THINK.xlsx")
    excel_table = ExcelTable(excel_source_info, "Discover_Sales_THINK")
    sql_parser_module.createTable("excel_yahoo_prices", excel_table)
    data = cursor.execute("select * from excel_yahoo_prices")
    # for i in data:
    #     print (i)

    
    connection_ibm = ibm_db.connect('DATABASE=aldonlm;''HOSTNAME=10.15.100.103;''PORT=50006;''PROTOCOL=TCPIP;''UID=aldondbi;''PWD=zf9j3Hw;', '', '')
    db2_source_info = DB2SourceInfo(connection_ibm)
    db2_table = DB2Table(db2_source_info, "App")
    sql_parser_module.createTable("App", db2_table)
    data = cursor.execute("select * from App")
    for i in data:
        print (i)
    # table = ibm_db.tables(connection_ibm)
    # r = ibm_db.fetch_both(table)
    # print (table.TABLE_SCHEMA)
    # while(r):
    #     print (r['TABLE_NAME'])
    #     r = ibm_db.fetch_both(table)
    # csvModule = CsvModule(connection)
    # csvModule = CsvModule(connection)
    # csv_resgiste_table_info  = CsvRegisteTableInfo("yahoo_prices" , csv_source_info)
    # csvModule.registeTable(csv_resgiste_table_info)
    # connection.createmodule("csv", csvModule)
    # cursor = connection.cursor()
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
    # csvModule = CsvModule(connection)

    # ctsi = CsvTableSourceInfo("csv_yahoo_prices1", "C:\\Users\\zhenl\\Documents\\yahoo_prices.csv")    
    # csvModule.createTable(cursor, ctsi)
    # data = cursor.execute("select Country, max(Volume) from csv_yahoo_prices1 where (Country = \'USA\' or Country = \'aaa\') and AdjClose > 25.63 and Volume > 40125200 group by Country")
    # for i in data:
    #     print (i)

if __name__ == '__main__':
    main()