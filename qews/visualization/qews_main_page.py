import dash
import dash_table
import dash_core_components as dcc
import dash_html_components as html
import plotly.graph_objs as go
import pandas as pd

from source_info.csv_source_info import *
from source_info.excel_source_info import *
from source_info.parquet_source_info import *
from source_info.sas_source_info import *
from qews import *
import os.path 


class VisSourceInfo:
    def __init__(self):
        self.qews = Qews()
        self.df_source = pd.DataFrame(data = {'current source': []})
        self.df_table_list = pd.DataFrame(data = {'table list': []})
        self.df_column_list = pd.DataFrame(data = {'column list': []})
    def add_source(self, source_name, source_info):
        if self.qews.add_source(source_name, source_info):
            self.df_source = self.df_source.append({'current source' : source_name}, ignore_index=True)
            return True
        return False

vis_source_info = VisSourceInfo()

def main_page(app):
    app.config['suppress_callback_exceptions']=True
    app.layout = html.Div(children=[
        html.H1(children = 'Hello Supper Select', style = {
            'textAlign': 'center',
            'color': '#7FDBFF'
        }),
        html.Div(children=[
        html.Div(children=[
            dash_table.DataTable(
            html.Br(),
            id = 'current_source_table',
            style_cell={'width': '150px', 'textAlign': 'center'},
            data=vis_source_info.df_source.to_dict('rows'),
            columns=[{'id': c, 'name': c} for c in vis_source_info.df_source.columns],
            style_table={
                'height': '550px',
                'overflowY': 'scroll',
                'border': 'thin lightgrey solid'
            },
        ),
        dash_table.DataTable(
            html.Br(),
            id = 'current_table_list',
            style_cell={'width': '150px', 'textAlign': 'center'},
            data=vis_source_info.df_table_list.to_dict('rows'),
            columns=[{'id': c, 'name': c} for c in vis_source_info.df_table_list.columns],
            style_table={
                'height': '550px',
                'overflowY': 'scroll',
                'border': 'thin lightgrey solid'
            },
        ),
        dash_table.DataTable(
            html.Br(),
            id = 'current_col_list',
            style_cell={'width': '150px', 'textAlign': 'center'},
            data=vis_source_info.df_column_list.to_dict('rows'),
            columns=[{'id': c, 'name': c} for c in vis_source_info.df_column_list.columns],
            style_table={
                'height': '550px',
                'overflowY': 'scroll',
                'border': 'thin lightgrey solid'
            },
        )],style={'columnCount': 3}),


        html.Div(children = [
        html.H3(children = 'Add Source', style = {'color' : '#7FDBFF'}),
        html.Label('Source type'),
        dcc.Dropdown(
            id = 'type_select',
            options=[
                {'label': 'excel', 'value': 'excel'},
                {'label': 'csv', 'value': 'csv'},
                {'label': 'db2', 'value': 'db2'},
                {'label': 'parquet', 'value': 'parquet'},
                {'label': 'sas', 'value': 'sas'},
            ],
            value = 'excel'
        ),
        html.Div(id = 'select_type_container'),
        html.Br(),
        html.Div(children= [html.Button('Add', id='add_source_button')])
        ],style = {'margin' : '100px'})
        ],style={'columnCount': 2}),
        html.Div(id='file_path_value_out_put', style = {'display' : 'none'}),

        html.Div(id='database_name_value_out_put', style = {'display' : 'none'}),
        html.Div(id='host_name_value_out_put', style = {'display' : 'none'}),
        html.Div(id='port_value_out_put', style = {'display' : 'none'}),
        html.Div(id='user_name_value_out_put', style = {'display' : 'none'}),
        html.Div(id='password_value_out_put', style = {'display' : 'none'}),

        html.Div(id='source_name_value_out_put', style = {'display' : 'none'}),
        html.Div(id='add_button_callback', style = {'display' : 'none'}),
        html.Br(),
        html.Br(),
        dcc.Textarea(
            id='sql_statement_area',
            placeholder='Enter a select sql...',
            value='',
            style={'width': '100%','height': '130px', 'fontSize':30}
        ),
        html.Br(),
        html.Button('run', id='run_select_button'),
        html.Br(),
        html.Br(),
        html.Div(id = 'display_area')
        ])

    @app.callback(dash.dependencies.Output('current_table_list', 'data'),
                 [dash.dependencies.Input('current_source_table', 'active_cell')])
    def update_table_list(active_cell):
        source_info = vis_source_info.qews.get_source_info(vis_source_info.df_source.iloc[active_cell[0], 0])
        vis_source_info.df_table_list = pd.DataFrame(data = {'table list': source_info.get_table_list()})
        return  vis_source_info.df_table_list.to_dict('rows')

    @app.callback(dash.dependencies.Output('current_table_list', 'active_cell'),
                 [dash.dependencies.Input('current_table_list', 'data')])
    def update_table_list(active_cell):
        return  [0, 0]

    @app.callback(dash.dependencies.Output('current_col_list', 'data'),
                 [dash.dependencies.Input('current_table_list', 'active_cell')],
                 [dash.dependencies.State('current_source_table', 'active_cell')])
    def update_col_list(table_active_cell, source_active_cell):
        source_info = vis_source_info.qews.get_source_info(vis_source_info.df_source.iloc[source_active_cell[0], 0])
        df_column_list = source_info.get_col_names_by_table_name(vis_source_info.df_table_list.iloc[table_active_cell[0], 0])
        print (vis_source_info.df_table_list.iloc[table_active_cell[0], 0])
        vis_source_info.df_column_list = pd.DataFrame(data = {'column list': df_column_list})
        return  vis_source_info.df_column_list.to_dict('rows')

    @app.callback(dash.dependencies.Output('current_source_table', 'data'),
                 [dash.dependencies.Input('add_source_button', 'n_clicks')],
                [dash.dependencies.State('file_path_value_out_put', 'children'),
                dash.dependencies.State('type_select', 'value'),
                dash.dependencies.State('source_name_value_out_put', 'children'),
                dash.dependencies.State('database_name_value_out_put', 'children'),
                dash.dependencies.State('host_name_value_out_put', 'children'),
                dash.dependencies.State('port_value_out_put', 'children'),
                dash.dependencies.State('user_name_value_out_put', 'children'),
                dash.dependencies.State('password_value_out_put', 'children')])
    def upload_data_path(n_clicks, file_path, file_type, source_name, database_name, host_name, port, user_name, password):
        if file_path == None:
            return vis_source_info.df_source.to_dict('rows')

        if file_type in ['excel', 'csv', 'parquet', 'sas'] : 
            if not os.path.exists(file_path):
                raise Exception(file_path + ' not exsit')
            if file_type == 'csv':
                if os.path.splitext(file_path)[1] != '.csv':
                    print (os.path.splitext(file_path)[1])
                    raise Exception(file_path + ' is not csv')
                csv_source_info = CsvSourceInfo(file_path)
                vis_source_info.add_source(source_name, csv_source_info)
            elif file_type == 'excel':
                if os.path.splitext(file_path)[1] != '.xlsx':
                    print (os.path.splitext(file_path)[1])
                    raise Exception(file_path + ' is not excel')
                excel_source_info = ExcelSourceInfo(file_path)
                vis_source_info.add_source(source_name, excel_source_info)
            elif file_type == 'parquet':
                if os.path.splitext(file_path)[1] != '.parquet':
                    print (os.path.splitext(file_path)[1])
                    raise Exception(file_path + ' is not parquet')
                parquet_source_info = ParquetSourceInfo(file_path)
                vis_source_info.add_source(source_name, parquet_source_info)
            elif file_type == 'sas':
                if os.path.splitext(file_path)[1] != '.sas7bdat':
                    print (os.path.splitext(file_path)[1])
                    raise Exception(file_path + ' is not sas7bdat')
                sas_source_info = SasSourceInfo(file_path)
                vis_source_info.add_source(source_name, sas_source_info)
        return vis_source_info.df_source.to_dict('rows')


    @app.callback(
    dash.dependencies.Output('select_type_container', 'children'),
    [dash.dependencies.Input('type_select', 'value')])
    def update_output(value):
        print (value)
        if value in ['excel', 'csv', 'parquet', 'sas'] :
            div = html.Div(children = [
                html.Label('source name'),
                dcc.Input(
                    id = 'source_name',
                    placeholder = 'Enter a source name...',
                    style={'height' : '30px',
                    'width': '300px'},
                    value=''),
                html.Br(),
                html.Label('file path'),
                dcc.Input(
                    id = 'file_path',
                    placeholder = 'Enter a file path...',
                    style={'height' : '30px',
                    'width': '800px'},
                    value=''),
                html.Br()],
                style={'columnCount': 1})

            return div
        elif value == 'db2':
                div = html.Div(children = [
                    html.Label('source name'),
                    dcc.Input(
                        id = 'source_name',
                        placeholder = 'Enter a source name...',
                        style={'height' : '30px',
                        'width': '300px'},
                        value=''),
                    html.Label('database name'),
                    dcc.Input(
                        id = 'database_name',
                        placeholder = 'Enter a database name...',
                        style={'height' : '30px',
                        'width': '300px'},
                        value=''),
                    html.Br(),
                    html.Label('host name'),
                    dcc.Input(
                        id = 'host_name',
                        placeholder = 'Enter a host name...',
                        style={'height' : '30px',
                        'width': '300px'},
                        value=''),
                    html.Br(),
                    html.Label('port'),
                    dcc.Input(
                        id = 'port',
                        placeholder = 'Enter a port...',
                        style={'height' : '30px',
                        'width': '300px'},
                        value=''),

                    html.Br(),
                    html.Label('user name'),
                    dcc.Input(
                        id = 'user_name',
                        placeholder = 'Enter a user name...',
                        style={'height' : '30px',
                        'width': '300px'},
                        value=''),
                    html.Br(),
                    html.Label('password'),
                    dcc.Input(
                        id = 'password',
                        placeholder = 'Enter a password...',
                        style={'height' : '30px',
                        'width': '300px'},
                        value='')
                    ],  
                    style={'columnCount': 1}) 
                return div

    @app.callback(dash.dependencies.Output('file_path_value_out_put', 'children'),
                 [dash.dependencies.Input('file_path', 'value')])
    def file_path(value):
        return value

    @app.callback(dash.dependencies.Output('database_name_value_out_put', 'children'),
                 [dash.dependencies.Input('database_name', 'value')])
    def database_name(value):
        return value

    @app.callback(dash.dependencies.Output('host_name_value_out_put', 'children'),
                 [dash.dependencies.Input('host_name', 'value')])
    def host_name(value):
        return value   

    @app.callback(dash.dependencies.Output('port_value_out_put', 'children'),
                 [dash.dependencies.Input('port_value', 'value')])
    def port(value):
        return value    

    @app.callback(dash.dependencies.Output('user_name_value_out_put', 'children'),
                 [dash.dependencies.Input('user_name', 'value')])
    def user_name(value):
        return value   

    @app.callback(dash.dependencies.Output('password_value_out_put', 'children'),
                 [dash.dependencies.Input('password', 'value')])
    def password(value):
        return value   


    @app.callback(dash.dependencies.Output('source_name_value_out_put', 'children'),
                 [dash.dependencies.Input('source_name', 'value')])
    def file_path(value):
        return value

    @app.callback(dash.dependencies.Output('display_area', 'children'),
                 [dash.dependencies.Input('run_select_button', 'n_clicks')],
                 [dash.dependencies.State('sql_statement_area', 'value')])
    def run_sql(n_clicks, sql):
        if sql == None:
            return
        print (11111)
        print (sql)
        ret = vis_source_info.qews.execute(sql)
        print ('end')
        if isinstance (ret, list):
            return ret
        else:
            data_table = dash_table.DataTable(
                id = __name__,
                style_cell={'width': '150px', 'textAlign': 'center', 'backgroundColor':'#EDFAFF'},
                style_header={'backgroundColor': '#a1c3d1'},
                data= ret.to_dict('rows'),
                columns=[{'id': c, 'name': c} for c in ret.columns],
                style_table={
                    'height': '500px',
                    'overflowY': 'scroll',
                    'border': 'thin lightgrey solid'
                },
            )
        return data_table

    # @app.callback(dash.dependencies.Output('display_area', 'children'), 
    #     [dash.dependencies.Input('run_select_button', 'n_clicks')],
    #     [dash.dependencies.State('sql_statement_area', 'value')])
    # def update_output(n_clicks, sql):
    #     df = vis_source_info.qews.execute(sql)
    #     fig = dcc.Graph(
    #         figure=go.Figure(
    #         data = [ go.Table(
    #                 header=dict(values=df.columns.tolist(), 
    #                     line = dict(color='#7D7F80'),
    #                     fill = dict(color='#a1c3d1')),
    #                 cells=dict(values=[df[value_name].values.tolist() for value_name in df.columns.tolist()],                
    #                 line = dict(color='#7D7F80'),
    #                 fill = dict(color='#EDFAFF'),
    #                 font = dict(color = '#506784', size = 12),
    #                 height = 25)
    #         )],
    #         layout = dict(width=1500, height=800)
    #         )
    #         )
    #     return [fig]

    # @app.callback(dash.dependencies.Output('display_area', 'chlidren'),  
    #             events=[dash.dependencies.Event('run_select_button', 'click')])
    # def run_sql():
    #     # data_frame = vis_source_info.qews.execute(sql_statement)
    #     return html.Label('1111')
        # return 'You have selected "{}"'.format(value)
# database_name = None, host_name = None, port = None, protocol = None, user_name = None, password = None
    return app
