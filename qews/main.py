import pandas as pd
import time
from qews import *
from source_info.csv_source_info import *
from source_info.excel_source_info import *
from source_info.pandas_source_info import *
from visualization.qews_main_page import main_page
from pandasql import sqldf







def main():
    external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css',"https://codepen.io/chriddyp/pen/dZMMma.css", "https://rawgit.com/lwileczek/Dash/master/undo_redo5.css"]
    app = dash.Dash(__name__, external_stylesheets=external_stylesheets)
    main_page(app)
    app.run_server(debug=False)

    # qews = Qews()
    # # time_start1 = time.time()
    # # # df_excel = pd.read_excel("example_data/Discover_Sales_THINK.xlsx", sheet_name = "Discover_Sales_THINK")
    # excel_source_info = ExcelSourceInfo("example_data/Discover_Sales_THINK.xlsx")
    # # excel_table = ExcelTable("Discover_Sales_THINK", excel_source_info)
    # # qews.registeTable("pandas", excel_table)

    # # excel_source_info = ExcelSourceInfo("example_data/Discover_Sales_THINK.xlsx")
    # # excel_table = ExcelTable("Discover_Sales_THINK", excel_source_info)
    # # qews.registeTable("pandas1", excel_table)
    # # # df_excel = pd.read_excel("example_data/Discover_Sales_THINK.xlsx", sheet_name = "Discover_Sales_THINK")
    # # # excel_source_info = PandasSourceInfo(df_excel)
    # # # excel_table = PandasTable(excel_source_info)
    # # # qews.registeTable("pandas1", excel_table)
    # csv_source_info = CsvSourceInfo("example_data/yahoo_prices.csv")
    # qews.add_source('csv', csv_source_info)
    # qews.add_source('excel', excel_source_info)
    # print (qews.execute('select csv_yahoo_prices.Volume,a.City from csv_yahoo_prices join excel_Discover_Sales_THINK as a on a.Country = csv_yahoo_prices.Country'))
    # # excel_table = CsvTable("yahoo_prices", excel_source_info)
    # # qews.registeTable("yahoo_prices", excel_table)

    # # excel_source_info = CsvSourceInfo("example_data/yahoo_prices1.csv")
    # # excel_table = CsvTable("yahoo_prices1", excel_source_info)
    # # qews.registeTable("yahoo_prices1", excel_table)
    # time_start1 = time.time()
    # print (qews.execute("select Formular_add_days(pandas.OrderDate, 3) from pandas join pandas1 on pandas.ProfitMargin = pandas1.ProfitMargin"))
    # # qews.execute("select y.AdjClose from (select * from yahoo_prices) y join yahoo_prices1 y1 on y.Volume = y1.Volume")
    # # print (qews.execute("select y.AdjClose from yahoo_prices y join (select Volume from  yahoo_prices1) y1 on y.Volume = y1.Volume"))
    # # print (qews.execute("select * from yahoo_prices1"))
    # time_end1 = time.time()
    # print('totally cost1',time_end1 - time_start1)



    # parquet_file = pq.ParquetFile("example_data/userdata1.parquet")
    # print (parquet_file.schema.names)
   


    # time_start2 = time.time()
    # # df = pd.read_csv("example_data/yahoo_prices.csv")
    # df_excel = pd.read_excel("example_data/Discover_Sales_THINK.xlsx", sheet_name = "Discover_Sales_THINK")
    # print (sqldf("select * from df_excel where Region = \'WA\'"))
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
