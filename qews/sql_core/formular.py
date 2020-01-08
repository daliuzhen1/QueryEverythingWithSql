import math
from datetime import timedelta
import datetime
import pandas as pd
class Formular():

    @staticmethod
    def Create(sqlite3_connection):
        sqlite3_connection.create_function("Formular_abs", 1, Formular.fomular_abs)
        sqlite3_connection.create_function("Formular_acos", 1, Formular.fomular_acos)
        sqlite3_connection.create_function("Formular_asc", 1, Formular.fomular_asc)
        sqlite3_connection.create_function("Formular_asin", 1, Formular.fomular_asin)
        sqlite3_connection.create_function("Formular_atan", 1, Formular.fomular_atan)
        sqlite3_connection.create_function("Formular_ceil", 1, Formular.fomular_ceil)
        sqlite3_connection.create_function("Formular_cos", 1, Formular.fomular_cos)
        sqlite3_connection.create_function("Formular_dand", -1, Formular.fomular_dand)
        

    @staticmethod   
    def fomular_abs(arg):
        try:
            if isinstance(arg,str):
                return abs(eval(arg))
            else:
                return abs(eval(arg))
        except Exception as ex:
            return None

    @staticmethod   
    def fomular_acos(arg):
        try:
            return math.acos(eval(arg))
        except Exception as ex:
            return None

    @staticmethod   
    def fomular_asc(arg):
        try:
            char = arg[0]
            return ord(char)
        except Exception as ex:
            return None

    @staticmethod   
    def fomular_asin(arg):
        try:
            return math.asin(eval(arg))
        except Exception as ex:
            return None

    @staticmethod   
    def fomular_atan(arg):
        try:
            return math.atan(eval(arg))
        except Exception as ex:
            return None

    @staticmethod   
    def fomular_ceil(arg):
        try:
            return math.ceil(eval(arg))
        except Exception as ex:
            return None

    @staticmethod   
    def fomular_cos(arg):
        try:
            return math.cos(eval(arg))
        except Exception as ex:
            return None

    @staticmethod   
    def fomular_dand(*args):
        try:
            for i in args:
                if i == False:
                    return 'False'
            return 'True'
        except Exception as ex:
            return None


    @staticmethod   
    def fomular_datediff(date1, date2, _type):
        try:
            d1 = datetime.datetime.strptime(date1)
            d2 = datetime.datetime.strptime(date2)
            if _type == 'm':
                return abs((d2 - d1).microseconds)
            elif _type == 's':
                return abs((d2 - d1).seconds)
            elif _type == 'd':
                return abs((d2 - d1).days)
        except Exception as ex:
            return None

    @staticmethod   
    def fomular_dayofmonth(date):
        try:
            print (date)
            d1 = datetime.datetime.strptime(date)
            print (d1)
            return d1.day
        except Exception as ex:
            print (ex)
            return None