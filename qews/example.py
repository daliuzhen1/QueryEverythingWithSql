import pandas as pd
import time
from qews import *
from source_info.csv_source_info import *
from source_info.excel_source_info import *
from pandasql import sqldf


import pyarrow.parquet as pq

def main():

    # qews = Qews()
    # time_start1 = time.time()
    # excel_source_info = ExcelSourceInfo("example_data/Discover_Sales_THINK.xlsx")
    # # df_excel = pd.read_excel("example_data/Discover_Sales_THINK.xlsx", sheet_name = "Discover_Sales_THINK")

    # excel_table = ExcelTable("Discover_Sales_THINK", excel_source_info)
    # qews.registeTable("df_excel", excel_table)

    # print (qews.execute("select * from df_excel where Region = \'WA\'"))
    # time_end1 = time.time()
    # print('totally cost1',time_end1 - time_start1)
    parquet_file = pq.ParquetFile("example_data/userdata1.parquet")
    print (parquet_file.schema.names)
   


    # time_start2 = time.time()
    # # df = pd.read_csv("example_data/yahoo_prices.csv")
    # df_excel = pd.read_excel("example_data/Discover_Sales_THINK.xlsx", sheet_name = "Discover_Sales_THINK")
    # print (sqldf("select * from df_excel"))
    # time_end2 = time.time()
    # print('totally cost1',time_end2 - time_start2)

    # time_start1 = time.time()
    # df_excel = pd.read_excel("example_data/Discover_Sales_THINK.xlsx", sheet_name = "Discover_Sales_THINK", use_cols = [3])
    # time_end1 = time.time()
    # print('totally cost1',time_end1 - time_start1)
    # time_start1 = time.time()
    # qews = Qews()

    # csv_source_info = CsvSourceInfo("example_data/yahoo_prices.csv")
    # csv_table = CsvTable("yahoo_prices", csv_source_info)
    # qews.registeTable("df", csv_table)

    # print (qews.execute("select Volume from yahoo_prices"))



if __name__ == '__main__':
    main()