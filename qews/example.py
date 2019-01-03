from sql_parser.csv_sql_parser import *
from sql_parser.excel_sql_parser import *
from sql_parser.pandas_sql_parser import *
from sql_parser.db2_sql_parser import *
from sql_parser.sas_sql_parser import *
from sql_parser.parquet_sql_parser import *
from sql_parser.sql_parser import *
import ibm_db
import pandas as pd

def main():
    df = pd.read_csv("C:\\Users\\zhenl\\Documents\\yahoo_prices.csv")
    connection=apsw.Connection(":memory:")
    sql_parser_module = SQLParserModule(connection, "sql_parser_module")
    pd_source_info = PandasSourceInfo(df)
    pd_table = PandasTable(pd_source_info, ["Volume", "AdjClose"])
    sql_parser_module.registeTable("yahoo_prices", pd_table)
    data = pd.read_sql("select * from yahoo_prices", connection)
    print (data)

    csv_source_info = CsvSourceInfo("C:\\Users\\zhenl\\Documents\\yahoo_prices.csv")
    csv_table = CsvTable(csv_source_info, ["Volume"])
    sql_parser_module.registeTable("csv_yahoo_prices", csv_table)
    data = pd.read_sql("select * from csv_yahoo_prices", connection)
    print (data)

    excel_source_info = ExcelSourceInfo("C:\\Users\\zhenl\\Desktop\\Discover_Sales_THINK.xlsx")
    excel_table = ExcelTable(excel_source_info, "Discover_Sales_THINK", ["Country", "ProductName", "Order Date"])
    sql_parser_module.registeTable("Discover_Sales_THINK", excel_table)
    data = pd.read_sql("select * from Discover_Sales_THINK", connection)
    print (data)

    sas_source_info = SasSourceInfo("C:\\Users\\zhenl\\Downloads\\tax.sas7bdat")
    sas_table = SasTable(sas_source_info, ["TAX88", "INC88"])
    sql_parser_module.registeTable("sas", sas_table)
    data = pd.read_sql("select * from sas", connection)
    print (data)

    parquet_source_info = ParquetSourceInfo("C:\\Users\\zhenl\\Downloads\\userdata1.parquet")
    parquet_table = ParquetTable(parquet_source_info, ["id"])
    sql_parser_module.registeTable("parquet", parquet_table)
    data = pd.read_sql("select * from parquet", connection)
    print (data)

    connection_ibm = ibm_db.connect('DATABASE=aldonlm;''HOSTNAME=10.15.10.10;''PORT=50006;''PROTOCOL=TCPIP;''UID=aaa;''PWD=aaa;', '', '')
    db2_source_info = DB2SourceInfo(connection_ibm)
    db2_table = DB2Table(db2_source_info, "App", ['appid', 'APPNM'])
    sql_parser_module.registeTable("App", db2_table)
    data = pd.read_sql("select appid, appnm from App", connection)
    print (data)


if __name__ == '__main__':
    main()