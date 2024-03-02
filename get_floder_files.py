import os
import time
import datetime
from load_folder_files import *



def monitor_directory(path, interval):
    while True:
        try:
            file_list = os.listdir(path)
            for filename in file_list:
                if filename and ('xlsx' in filename or 'csv' in filename):
                    print(f"File Found: {filename}")
                    load_data()
                    # print(f"Found files: {filename}")
                else:
                    print("No file found.")
                    
                    
        except Exception as e:
            print(f'ERROR: {e}')
        finally:
            time.sleep(interval)

if __name__ == "__main__":
    # directory_path = r"C:/Users/TECHOPTIMA/Desktop/FrontendFiles" 
    directory_path = r'C:/Users/ADMIN/Desktop/shared_files'
    check_interval = 60  # 1 minute in seconds
    
    monitor_directory(directory_path, check_interval)
