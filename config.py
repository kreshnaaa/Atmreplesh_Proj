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

SOURCE_PATH = 'C:/Users/ADMIN/Desktop/ofc_projects/atm_project/files_folder'
ARCHIVE_DIRECTORY = "C:/Users/ADMIN/Desktop/ofc_projects/atm_project/files_folder/archive"
STORED_FILES = 'C:/Users/ADMIN/Desktop/ofc_projects/atm_project/files_folder/convert_files'

# Email account credentials and server details
IMAP_SERVER = 'imap.gmail.com'
EMAIL = "filesemail806@gmail.com" 
PASSWORD = 'fkss wbwm pqii chhs'
"MAINPW = filesem@iLs77"

#---------------------------------------------------------------------------------------------------------
                            # Local connection
#---------------------------------------------------------------------------------------------------------

# driver = 'ODBC Driver 17 for SQL Server'
# server = 'LAPTOP-MKS4BNBQ'
# database = 'ATM_DATA'
# connection_string = f"DRIVER={driver};SERVER={server};DATABASE={database};Trusted_Connection=yes;"
# engine = create_engine(f"mssql+pyodbc:///?odbc_connect={connection_string}", fast_executemany=True)
# inspector = inspect(engine)


# SOURCE_PATH = 'C:/Users/ADMIN/Desktop/files_folder'
# ARCHIVE_DIRECTORY = "C:/Users/ADMIN/Desktop/files_folder/archive"
# STORED_FILES = 'C:/Users/ADMIN/Desktop/files_folder/convert_files'


# # Email account credentials and server details
# IMAP_SERVER = 'imap.gmail.com'
# EMAIL = "filesemail806@gmail.com" 
# PASSWORD = 'hjtojolbxenednyx'


