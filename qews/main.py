import sys

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
    app.run_server(debug=False）

if __name__ == '__main__':
    main()
