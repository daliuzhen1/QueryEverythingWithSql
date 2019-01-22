from cx_Freeze import setup, Executable
import os
base = None    

executables = [Executable("example.py", base=base)]

packages = ["jinja2", "pyarrow", "pkg_resources","asyncio", "sqlite3", "os","idna", "fastparquet", "pandas", "sas7bdat", "sqlite3", "re", "sqlalchemy", "contextlib", "inspect", "dash", "dash_table", "dash_core_components", "dash_html_components", "plotly", "numpy"]
options = {
    'build_exe': {    
        'packages':packages,
    },    
}

os.environ['TCL_LIBRARY'] = r'C:\Users\zhenl\AppData\Local\Programs\Python\Python36\tcl\tcl8.6'
os.environ['TK_LIBRARY'] = r'C:\Users\zhenl\AppData\Local\Programs\Python\Python36\tcl\tk8.6'

setup(
    name = "supper_select",
    options = options,
    version = "0.01",
    description = 'zhen',
    executables = executables
)