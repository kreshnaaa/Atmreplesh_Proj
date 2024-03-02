from sqlalchemy import create_engine, inspect
import pyodbc

#--------------------------------------------------------------------
                         #server connection
#--------------------------------------------------------------------

server='192.168.29.128\SQLEXPRESS'
database='TrackingSolutions'
#database = 'ATM_DATA'
username='Krishna'
password='Kreshna@555'

driver = 'ODBC Driver 17 for SQL Server'
connection_string = f"DRIVER={driver};SERVER={server};DATABASE={database};UID={username};PWD={password}"
engine = create_engine(f"mssql+pyodbc:///?odbc_connect={connection_string}", fast_executemany=True)
# inspector = inspect(engine)
source_path = 'C:/Users/ADMIN/Desktop/ofc_projects/shared_files'
archive_fol = 'C:/Users/ADMIN/Desktop/ofc_projects/shared_files/archive'
conv_files = 'C:/Users/ADMIN/Desktop/ofc_projects/shared_files/convert_files'
