from config import *
import os
import shutil
import re
import pandas as pd
from googletrans import Translator
from datetime import datetime
from multiprocessing import Pool, cpu_count


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

translator = Translator(service_urls=['translate.google.com'])
translation_cache = {}  # To cache translations
CHUNK_SIZE = 100
def translate_arabic_to_english(text):
    if text in translation_cache:
        return translation_cache[text]
    else:
        translation = translator.translate(text, dest='en', src='ar').text
        translation_cache[text] = translation
        return translation
def process_rows(chunk):
    translated_rows = []
    for row in chunk.iterrows():
        translated_row = row[1].apply(lambda x: translate_arabic_to_english(x) if isinstance(x, str) and re.search('[\u0600-\u06FF]', x) else x)
        translated_rows.append(translated_row)
    return translated_rows
def translate_columns(translated_df):
    translated_columns = [translate_arabic_to_english(col) if isinstance(col, str) and re.search('[\u0600-\u06FF]', col) else col for col in translated_df.columns]
    translated_df.columns = translated_columns

def translate_values(translated_df):
    translated_df = translated_df.applymap(lambda x: translate_arabic_to_english(x) if isinstance(x, str) and re.search('[\u0600-\u06FF]', x) else x)
    return translated_df
def process_file(file_path, email_sub, file_name):
    try:
        df = pd.read_excel(file_path)  # Assuming the file is in Excel format
        chunks = [df[i:i + CHUNK_SIZE] for i in range(0, len(df), CHUNK_SIZE)]
        with Pool(cpu_count()) as pool:
            results = pool.map(process_rows, chunks)
        translated_rows = [row for chunk in results for row in chunk]
        translated_df = pd.DataFrame(translated_rows, columns=df.columns)

        translate_columns(translated_df)
        translated_df = translate_values(translated_df)

        current_time = datetime.now().strftime('%H%M%S')
        filename = f"{current_time}_{file_name}"
        
        translated_df.to_excel(f'C:/Users/ADMIN/Desktop/ofc_projects/atm_project/files_folder/convert_files/{filename}', index=False)

        header_df = pd.read_sql("SELECT * FROM Stg_Bank_Details", con=engine)
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
                        '10_Amounts', '50_Amounts', '100_Amounts', '200_Amount', '500_Amount', 
                        'Total_Repl_Amount']
        
        datavalues_df.columns = columns_names

        # Get the "total" row from the DataFrame
        total_row = datavalues_df.loc[datavalues_df['Team_no'] == 'TOTAL']
        
        # Access specific values within the "total" row using negative indexing
        values = total_row.iloc[:, -5:-1].values.tolist()
        
        list_values = values[[0][0]]
        
        flat_list = [float(value) for value in list_values]
        
        # Calculate the sum of the float values
        total = sum(flat_list)
        

        column_values = {
            'SlNo': [header_id+1],
            'Bank_name': [email_sub],
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

        filtered_data.loc[:, 'isSaved'] = "N"
        filtered_data.loc[:, 'isSaved'].fillna("N", inplace=True)

        
        filtered_data.to_sql("Stg_ATM_Details", engine, if_exists='append',index=False)
        
        alert_notification()
        move_file_to_archive(file_name, filename)
    except Exception as e:
        print(f'Error: {e}')
        print('Sorry for the inconvenience!')

# def load_data(stored_subj, filenames):
#     for strd_subj,email_file  in zip(stored_subj,filenames):
#         for file_name in os.listdir(SOURCE_PATH):
#             file_path = os.path.join(SOURCE_PATH, file_name)
        
#         # Check if the file is a CSV file
#             if file_path.endswith('.csv') or file_path.endswith('.xlsx'):
#                 process_file(file_path,strd_subj,email_file)
# except Exception as e:
#         print(f'Error: {e}')
#         print('Sorry for the inconvenienc(e!')    




#def load_data(stored_subj, filenames):
    

# def load_data(stored_subj, filenames):
#     translator = Translator(service_urls=['translate.google.com'])

#     for file_name, strd_subj, email_file in zip(os.listdir(SOURCE_PATH), stored_subj, filenames):
#         file_path = os.path.join(SOURCE_PATH, file_name)

#         if file_name.endswith('.csv') or file_name.endswith('.xlsx'):
#             try:
#                 translated_df = pd.read_excel(file_path)

#                 # Function to translate Arabic words to English
#                 def translate_arabic_to_english(text):
#                     translation = translator.translate(text, dest='en', src='ar')
#                     return translation.text

#                 # Translate Arabic headers to English
#                 translated_columns = [translate_arabic_to_english(col) if isinstance(col, str) and re.search('[\u0600-\u06FF]', col) else col for col in translated_df.columns]

#                 # Update column names with translated headers
#                 translated_df.columns = translated_columns

#                 # Translate Arabic values in the data to English
#                 def batch_translate(batch):
#                     translations = translator.translate(batch.tolist(), dest='en', src='ar')
#                     return [translation.text for translation in translations]

#                 for col in translated_df.columns:
#                     if translated_df[col].dtype == 'object':
#                         mask = isinstance(translated_df[col], str) & translated_df[col].str.contains('[\u0600-\u06FF]')
#                         translated_df.loc[mask, col] = batch_translate(translated_df.loc[mask, col])

#                 # Rest of your code...

#                 current_time = datetime.now().strftime('%H%M%S')
#                 filename = f"{current_time}_{email_file}"
#                 translated_df.to_excel(f'C:/Users/ADMIN/Desktop/ofc_projects/atm_project/files_folder/convert_files/{filename}', index=False)
#                 header_df = pd.read_sql("SELECT * FROM Stg_Bank_Details", con = engine)
#                 # Retrieve the maximum value of the 'SlNo' column
#                 header_id = header_df['SlNo'].max()
#                 if pd.isna(header_id):
#                     header_id = 0
#                 # Assign the last row out of the first 6 rows as the header
#                 header_df = translated_df[:5].iloc[-1]
#                 datavalues_df = translated_df[6:]
#                 datavalues_df.columns = header_df.values.tolist()

#                 # Assign columns_names as the column names
#                 columns_names = ['Team_no', 'ATM_ID', 'ATM_TYPE', 'ATM_Location', '10_Denom_Notes',
#                                 '50_Denom_Notes', '100_Denom_Notes', '200_Denom_Notes', '500_Denom_Notes',
#                                 '10_Amounts', '50_Amounts', '100_Amounts', '200_Amount', '500_Amount', 'Total_Repl_Amount']
#                 datavalues_df.columns = columns_names 
#                 # Get the "total" row from the DataFrame
#                 total_row = datavalues_df.loc[datavalues_df['Team_no'] == 'TOTAL']
                
#                 # Access specific values within the "total" row using negative indexing
#                 values = total_row.iloc[:, -5:-1].values.tolist()
#                 list_values = values[[0][0]]
#                 flat_list = [float(value) for value in list_values]
#                 # Calculate the sum of the float values
#                 total = sum(flat_list)
#                 # value = translated_df.iloc[2, 3]
#                 # formatted_date = pd.to_datetime(value).strftime('%Y-%m-%d')
        
#                 column_values = {
#                     'SlNo': [header_id+1],
#                     'Bank_name': [strd_subj],
#                     'city': [translated_df.iloc[0, 1]],
#                     'Day': [translated_df.iloc[1, 1]],
#                     'Date': [translated_df.iloc[2, 1]],
#                     'company_name': [translated_df.iloc[0, 4]],
#                     'number_of_atms_fed': [translated_df.iloc[1, 4]],
#                     'cash_center': [translated_df.iloc[0, 11]],
#                     'team_number': [translated_df.iloc[1, 11]],
#                     '50_Amounts' : [list_values[0]],
#                     '100_Amounts' : [list_values[1]],
#                     '200_Amounts' : [list_values[2]],
#                     '500_Amounts' : [list_values[3]],
#                     'TOTAL' : [total]
#                 }

#                 new_dataframe = pd.DataFrame(column_values)
#                 new_dataframe["isSaved"] = "N"
#                 new_dataframe['isSaved'].fillna("N", inplace=True)
#                 new_dataframe.to_sql('Stg_Bank_Details', engine, if_exists='append', index=False)
                

#                 datavalues_df = datavalues_df.drop('10_Denom_Notes', axis=1)
#                 datavalues_df = datavalues_df.drop('10_Amounts', axis=1)
#                 filtered_data = datavalues_df[datavalues_df['ATM_ID'] != None]
#                 filtered_data = filtered_data.head(2)
#                 filtered_data.loc[:, 'Header_id'] = header_id+1
#                 filtered_data.loc[:, 'Header_id'].fillna(header_id+1, inplace=True)
#                 # filtered_data.loc[:, 'staging_id'] = "STG001"
#                 # filtered_data.loc[:, 'staging_id'].fillna("STG001", inplace=True)
#                 filtered_data.loc[:, 'isSaved'] = "N"
#                 filtered_data.loc[:, 'isSaved'].fillna("N", inplace=True)

#                 # print(new_dataframe1)
#                 filtered_data.to_sql("Stg_ATM_Details", engine, if_exists='append',index=False)#index=False)                   
#                 alert_notification()
#                 move_file_to_archive(file_name,filename)

#             except Exception as e:
#                 print(f'Error: {e}')
#                 print('Sorry for the inconvenience! The program will continue running.')