import dash
import dash_table
import dash_core_components as dcc
import dash_html_components as html
import plotly.graph_objs as go
import numpy as np

def create_scatter(X_list, Y_list):
    trace = go.Scatter(
        x = X_list,
        y = Y_list,
        mode = 'markers'
    )
    graph = dcc.Graph(
        figure=go.Figure(
        data = [trace],
        layout = dict(width=1000, height=750)
        )
    )
    return graph

def create_line(X_list, Y_list):
    trace = go.Scatter(
        x = X_list,
        y = Y_list
    )
    graph = dcc.Graph(
        figure=go.Figure(
        data = [trace],
        layout = dict(width=1000, height=750)
        )
    )
    return graph

def create_bar(X_list, Y_list):
    trace = go.Bar(
        x = X_list,
        y = Y_list
    )
    graph = dcc.Graph(
        figure=go.Figure(
        data = [trace],
        layout = dict(width=1000, height=750)
        )
    )
    return graph