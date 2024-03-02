from config import *
import os
import shutil
import re
import pandas as pd
from googletrans import Translator
from datetime import datetime

def move_file_to_archive(filename,filesname):

    source_file_path = os.path.join(SOURCE_PATH, filename)
    shutil.move(source_file_path, ARCHIVE_DIRECTORY)
    print("Original File moved into archive folder")
    
    source_file_path = os.path.join(STORED_FILES, filesname)
    shutil.move(source_file_path, ARCHIVE_DIRECTORY)
    print("Converted File moved into archive folder")

def alert_notification():

    original_table_name = 'Order_Alerts'
    # Create a new DataFrame with the updated slno values
    data = {'IsNewFileArrived': [1],
            'IsNewFileArrivedAlert': [0],
            'SlNo': [1]}  # Replace new_slno with the updated value you want

    updated_df = pd.DataFrame(data)

    
    engine.execute(f'DROP TABLE IF EXISTS {original_table_name};')

    # Create a new table with the same structure as the original, including the primary key constraint
    create_table_query = f'''
        CREATE TABLE {original_table_name} (
            IsNewFileArrived BIGINT,
            IsNewFileArrivedAlert BIGINT,
            SlNo INT PRIMARY KEY
        );
    '''
    engine.execute(create_table_query)

    # Insert the data from the updated DataFrame into the new table
    updated_df.to_sql(original_table_name, engine, index=False, if_exists='append')

    # columns = {
    #             'IsNewFileArrived': [1],
    #             'IsNewFileArrivedAlert':[0]
    #         }
    # alert_db = pd.DataFrame(columns)
    # alert_db.to_sql('Order_Alerts', engine, if_exists='replace',index=False)

def load_data(stored_subj,filenames):                         
# Iterate over the files in the folder
    # for email_sub, filesname in zip(stored_subj,filenames):
    for file_name,strd_subj,email_file  in zip(os.listdir(SOURCE_PATH),stored_subj,filenames):
        file_path = os.path.join(SOURCE_PATH, file_name)
        
        # Check if the file is a CSV file
        if file_name.endswith('.csv') or file_name.endswith('.xlsx'):
            # file.append(file_path)
            try:
                translated_df = pd.read_excel(file_path)

                translator = Translator(service_urls=['translate.google.com'])

                # Function to translate Arabic words to English
                def translate_arabic_to_english(text):
                    translation = translator.translate(text, dest='en', src='ar')
                    return translation.text

                # Translate Arabic headers to English
                translated_columns = [translate_arabic_to_english(col) if isinstance(col, str) and re.search('[\u0600-\u06FF]', col) else col for col in translated_df.columns]

                # Update column names with translated headers
                translated_df.columns = translated_columns

                # Translate Arabic values in the data to English
                for col in translated_df.columns:
                    if translated_df[col].dtype == 'object':
                        translated_df[col] = translated_df[col].apply(lambda x: translate_arabic_to_english(x) if isinstance(x, str) and re.search('[\u0600-\u06FF]', x) else x)
                # current_time = datetime.now().strftime('%Y%m%d_%H%M%S')
                current_time = datetime.now().strftime('%H%M%S')
                filename = f"{current_time}_{email_file}"
                translated_df.to_excel(f'C:/Users/ADMIN/Desktop/ofc_projects/atm_project/files_folder/convert_files/{filename}', index=False)
                header_df = pd.read_sql("SELECT * FROM Stg_Bank_Details", con = engine)
                # Retrieve the maximum value of the 'SlNo' column
                header_id = header_df['SlNo'].max()
                if pd.isna(header_id):
                    header_id = 0
                # Assign the last row out of the first 6 rows as the header
                header_df = translated_df[:5].iloc[-1]
                datavalues_df = translated_df[6:]
                datavalues_df.columns = header_df.values.tolist()

                # Assign columns_names as the column names
                columns_names = ['Team_no', 'ATM_ID', 'ATM_TYPE', 'ATM_Location', '10_Denom_Notes',
                                '50_Denom_Notes', '100_Denom_Notes', '200_Denom_Notes', '500_Denom_Notes',
                                '10_Amounts', '50_Amounts', '100_Amounts', '200_Amount', '500_Amount', 'Total_Repl_Amount']
                datavalues_df.columns = columns_names 
                # Get the "total" row from the DataFrame
                total_row = datavalues_df.loc[datavalues_df['Team_no'] == 'TOTAL']
                
                # Access specific values within the "total" row using negative indexing
                values = total_row.iloc[:, -5:-1].values.tolist()
                list_values = values[[0][0]]
                flat_list = [float(value) for value in list_values]
                # Calculate the sum of the float values
                total = sum(flat_list)
                # value = translated_df.iloc[2, 3]
                # formatted_date = pd.to_datetime(value).strftime('%Y-%m-%d')
        
                column_values = {
                    'SlNo': [header_id+1],
                    'Bank_name': [strd_subj],
                    'city': [translated_df.iloc[0, 1]],
                    'Day': [translated_df.iloc[1, 1]],
                    'Date': [translated_df.iloc[2, 1]],
                    'company_name': [translated_df.iloc[0, 4]],
                    'number_of_atms_fed': [translated_df.iloc[1, 4]],
                    'cash_center': [translated_df.iloc[0, 11]],
                    'team_number': [translated_df.iloc[1, 11]],
                    '50_Amounts' : [list_values[0]],
                    '100_Amounts' : [list_values[1]],
                    '200_Amounts' : [list_values[2]],
                    '500_Amounts' : [list_values[3]],
                    'TOTAL' : [total]
                }

                new_dataframe = pd.DataFrame(column_values)
                new_dataframe["isSaved"] = "N"
                new_dataframe['isSaved'].fillna("N", inplace=True)
                new_dataframe.to_sql('Stg_Bank_Details', engine, if_exists='append', index=False)
                

                datavalues_df = datavalues_df.drop('10_Denom_Notes', axis=1)
                datavalues_df = datavalues_df.drop('10_Amounts', axis=1)
                filtered_data = datavalues_df[datavalues_df['ATM_ID'] != None]
                filtered_data = filtered_data.head(2)
                filtered_data.loc[:, 'Header_id'] = header_id+1
                filtered_data.loc[:, 'Header_id'].fillna(header_id+1, inplace=True)
                # filtered_data.loc[:, 'staging_id'] = "STG001"
                # filtered_data.loc[:, 'staging_id'].fillna("STG001", inplace=True)
                filtered_data.loc[:, 'isSaved'] = "N"
                filtered_data.loc[:, 'isSaved'].fillna("N", inplace=True)

                # print(new_dataframe1)
                filtered_data.to_sql("Stg_ATM_Details", engine, if_exists='append',index=False)#index=False)                   
                alert_notification()
                move_file_to_archive(file_name,filename)
            except Exception as e:
                print(f'Error : {e}')
                print('Sorry For Inconvinence !!! ')  #The Program Is Going To Rerun)
                    # load_data(stored_subj)

# if __name__ == '__main__':
#     value = ['arabic bank', 'sbi bank']
#     # filenames = ['ganesh','venkey']
#     load_data(value)




# def move_file_to_archive(filename):

#     source_file_path = os.path.join(SOURCE_PATH, filename)
#     # Create the full file path
#     # file_path = os.path.join(ARCHIVE_DIRECTORY,filename)

#     # if not os.path.exists(file_path):
#         # Move the file to the archive directory
#     shutil.move(source_file_path, ARCHIVE_DIRECTORY)
#     # Delete the original file
#     # os.remove(source_file_path) 
#     print("File moved to archive folder")
    
    

#     # unique_id = datetime.now().strftime('%Y%m%d%H%M%S')

#     # # Append the unique identifier to the filename
#     # new_filename = f"{unique_id}_{filename}"
#     # source_file_path = os.path.join(SOURCE_PATH, new_filename)
#     # shutil.move(source_file_path, ARCHIVE_DIRECTORY)
#     # os.remove(source_file_path)
        
#     # except Exception as e:
#     #     os.remove(source_file_path)
#     #     print(f"{filename} already exists")

# def alert_notification():
#     columns = {
#                 'IsNewFileArrived': [1],
#                 'IsNewFileArrivedAlert':[0]
#             }
#     alert_db = pd.DataFrame(columns)
#     alert_db.to_sql('Order_Alerts', engine, if_exists='replace',index=False)

# def load_data(stored_subj):
# # Iterate over the files in the folder
#     for email_sub in stored_subj:
#         for file_name in os.listdir(SOURCE_PATH):
#             file_path = os.path.join(SOURCE_PATH, file_name)
            
#             # Check if the file is a CSV file
#             if file_name.endswith('.csv') or file_name.endswith('.xlsx'):
#                 # file.append(file_path)
#                 try:
#                     translated_df = pd.read_excel(file_path)
                    
#                     translator = Translator(service_urls=['translate.google.com'])

#                     # Function to translate Arabic words to English
#                     def translate_arabic_to_english(text):
#                         translation = translator.translate(text, dest='en', src='ar')
#                         return translation.text

#                     # Translate Arabic headers to English
#                     translated_columns = [translate_arabic_to_english(col) if isinstance(col, str) and re.search('[\u0600-\u06FF]', col) else col for col in translated_df.columns]

#                     # Update column names with translated headers
#                     translated_df.columns = translated_columns

#                     # Translate Arabic values in the data to English
#                     for col in translated_df.columns:
#                         if translated_df[col].dtype == 'object':
#                             translated_df[col] = translated_df[col].apply(lambda x: translate_arabic_to_english(x) if isinstance(x, str) and re.search('[\u0600-\u06FF]', x) else x)

#                     translated_df.to_excel(f'C:/Users/ADMIN/Desktop/files_folder/convert_files/{file_name}', index=False)
#                     header_df = pd.read_sql("SELECT * FROM Stg_Bank_Details", con = engine)
#                     # Retrieve the maximum value of the 'SlNo' column
#                     header_id = header_df['SlNo'].max()
#                     if pd.isna(header_id):
#                         header_id = 0
#                     # Assign the last row out of the first 6 rows as the header
#                     header_df = translated_df[:5].iloc[-1]
#                     datavalues_df = translated_df[6:]
#                     datavalues_df.columns = header_df.values.tolist()

#                     # Assign columns_names as the column names
#                     columns_names = ['Team_no', 'ATM_ID', 'ATM_TYPE', 'ATM_Location', '10_Denom_Notes',
#                                     '50_Denom_Notes', '100_Denom_Notes', '200_Denom_Notes', '500_Denom_Notes',
#                                     '10_Amounts', '50_Amounts', '100_Amounts', '200_Amount', '500_Amount', 'Total_Repl_Amount']
#                     datavalues_df.columns = columns_names 
#                     # Get the "total" row from the DataFrame
#                     total_row = datavalues_df.loc[datavalues_df['Team_no'] == 'TOTAL']
                    
#                     # Access specific values within the "total" row using negative indexing
#                     values = total_row.iloc[:, -5:-1].values.tolist()
#                     list_values = values[[0][0]]
#                     flat_list = [float(value) for value in list_values]
#                     # Calculate the sum of the float values
#                     total = sum(flat_list)
#                     # value = translated_df.iloc[2, 3]
#                     # formatted_date = pd.to_datetime(value).strftime('%Y-%m-%d')
            
#                     column_values = {
#                         'SlNo': [header_id+1],
#                         'Bank_name': [email_sub],
#                         'city': [translated_df.iloc[0, 1]],
#                         'Day': [translated_df.iloc[1, 1]],
#                         'Date': [translated_df.iloc[2, 1]],
#                         'company_name': [translated_df.iloc[0, 4]],
#                         'number_of_atms_fed': [translated_df.iloc[1, 4]],
#                         'cash_center': [translated_df.iloc[0, 11]],
#                         'team_number': [translated_df.iloc[1, 11]],
#                         '50_Amounts' : [list_values[0]],
#                         '100_Amounts' : [list_values[1]],
#                         '200_Amounts' : [list_values[2]],
#                         '500_Amounts' : [list_values[3]],
#                         'TOTAL' : [total]
#                     }

#                     new_dataframe = pd.DataFrame(column_values)
#                     new_dataframe["isSaved"] = "N"
#                     new_dataframe['isSaved'].fillna("N", inplace=True)
#                     new_dataframe.to_sql('Stg_Bank_Details', engine, if_exists='append', index=False)

#                     datavalues_df = datavalues_df.drop('10_Denom_Notes', axis=1)
#                     datavalues_df = datavalues_df.drop('10_Amounts', axis=1)
#                     filtered_data = datavalues_df[datavalues_df['ATM_ID'] != ''].copy()
#                     filtered_data = filtered_data.head(2)

#                     filtered_data.loc[:, 'Header_id'] = header_id+1
#                     filtered_data.loc[:, 'Header_id'].fillna(header_id+1, inplace=True)
#                     # filtered_data.loc[:, 'staging_id'] = "STG001"
#                     # filtered_data.loc[:, 'staging_id'].fillna("STG001", inplace=True)
#                     filtered_data.loc[:, 'isSaved'] = "N"
#                     filtered_data.loc[:, 'isSaved'].fillna("N", inplace=True)

#                     # print(new_dataframe1)
#                     filtered_data.to_sql("Stg_ATM_Details", engine, if_exists='append',index=False)#index=False)
#                     alert_notification()
#                     move_file_to_archive(file_name)
#                     break
#                 except Exception as e:
#                     print(f'Error : {e}')
#                     print('Sorry For Inconvinence !!! The Program Is Going To Rerun')
#                     load_data(stored_subj)
# # if __name__ == '__main__':
# #     value = ['arabic bank', 'sbi bank']
# #     load_data(value)