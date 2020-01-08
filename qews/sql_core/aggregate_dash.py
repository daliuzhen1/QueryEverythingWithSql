import math
from datetime import timedelta
import pandas as pd
from visualization.dash import *
import uuid

dash_dict = {} 

class Dash_Scatter:
    def __init__(self):
        self.X_list = []
        self.Y_list = []

    def step(self, X, Y):
        self.X_list.append(X)
        self.Y_list.append(Y)

    def finalize(self):
        dashID = self.__str__
        aaa = create_scatter(self.X_list, self.Y_list)
        dash_dict[dashID].append_plot(aaa)
        return None

class Dash_Line:
    def __init__(self):
        self.X_list = []
        self.Y_list = []

    def step(self, X, Y):
        self.X_list.append(X)
        self.Y_list.append(Y)

    def finalize(self):
        dashID = self.__str__
        line = create_line(self.X_list, self.Y_list)
        dash_dict[dashID].append_plot(line)
        return None

class Dash_Bar:
    def __init__(self):
        self.X_list = []
        self.Y_list = []

    def step(self, X, Y):
        self.X_list.append(X)
        self.Y_list.append(Y)

    def finalize(self):
        dashID = self.__str__
        bar = create_bar(self.X_list, self.Y_list)
        dash_dict[dashID].append_plot(bar)
        return None

class Dash():
    def __init__(self):
        self.plot_list = []

    @staticmethod
    def Create(sqlite3_connection):
        uuid_str = uuid.uuid4().hex
        Dash_Scatter.__str__ = uuid_str
        dash_dict[uuid_str] = Dash()
        sqlite3_connection.create_aggregate("Dash_Scatter", 2, Dash_Scatter)
        Dash_Line.__str__ = uuid_str
        sqlite3_connection.create_aggregate("Dash_Line", 2, Dash_Line)
        Dash_Bar.__str__ = uuid_str
        sqlite3_connection.create_aggregate("Dash_Bar", 2, Dash_Bar)
        return uuid_str

    def append_plot(self, plot):
        self.plot_list.append(plot)

    def get_plot_list(self):
        return self.plot_list

    @staticmethod
    def get_and_pop_dash_by_uuid(uuid):
        return dash_dict.pop(uuid, None)