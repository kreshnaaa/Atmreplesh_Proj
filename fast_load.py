import os
import shutil
import re
import pandas as pd
from googletrans import Translator
from datetime import datetime
import time
import concurrent.futures

def load_data():
    start = time.time()
    file_path = 'C:/Users/ADMIN/Desktop/ofc_projects/atm_project/input_files/ATM_Replenishment_Request.xlsx'

    try:
        translated_df = pd.read_excel(file_path, engine='openpyxl')

        translator = Translator()

        def translate_text(text):
            try:
                translation = translator.translate(text, src='ar', dest='en')
                return translation.text
            except Exception as e:
                print(f"Translation Error: {e}")
                return text  # Return the original text on error

        def translate_column(column):
            if isinstance(column, pd.Series):
                return column.apply(lambda x: translate_text(x) if isinstance(x, str) and re.search('[\u0600-\u06FF]', str(x)) else x)
            return column

        # Translate each column of the DataFrame using parallel processing
        with concurrent.futures.ThreadPoolExecutor() as executor:
            translated_columns = list(executor.map(translate_column, translated_df.columns))

        # Create a new DataFrame with the translated columns
        translated_df = pd.DataFrame(translated_columns).transpose()
        translated_df.index = translated_df.columns  # Set the index to match the columns

        email_file = 'ATM_Replenishment_Request.xlsx'
        current_time = datetime.now().strftime('%H%M%S')
        filename = f"{current_time}_{email_file}"
        translated_df.to_excel(f'C:/Users/ADMIN/Desktop/ofc_projects/atm_project/files_folder/convert_files/{filename}', index=True)

        end = time.time()
        total = end - start
        print(f"Translation completed in {total} seconds")
    except Exception as e:
        print(f'Error: {e}')
        print('Sorry for the inconvenience!')  # The program is going to rerun

load_data()





# import os
# import shutil
# import re
# import pandas as pd
# from googletrans import Translator
# from datetime import datetime
# import time

# def load_data():     
#     start=time.time()    
#     file_path='C:/Users/ADMIN/Desktop/ofc_projects/atm_project/input_files/ATM_Replenishment_Request.xlsx'
    
#     try:
#         translated_df = pd.read_excel(file_path,engine='openpyxl')

#         translator = Translator(service_urls=['translate.google.com'])

#         # Function to translate Arabic words to English
#         def translate_arabic_to_english(text):
#             translation = translator.translate(text, dest='en', src='ar')
#             return translation.text

#         # Translate Arabic headers to English
#         translated_columns = [translate_arabic_to_english(col) if isinstance(col, str) and re.search('[\u0600-\u06FF]', col) else col for col in translated_df.columns]

#         # Update column names with translated headers
#         translated_df.columns = translated_columns

#         # Translate Arabic values in the data to English
#         for col in translated_df.columns:
#             if translated_df[col].dtype == 'object':
#                 translated_df[col] = translated_df[col].apply(lambda x: translate_arabic_to_english(x) if isinstance(x, str) and re.search('[\u0600-\u06FF]', x) else x)
#         email_file='ATM_Replenishment_Request.xlsx' 
#         current_time = datetime.now().strftime('%H%M%S')
#         filename = f"{current_time}_{email_file}"
#         translated_df.to_excel(f'C:/Users/ADMIN/Desktop/ofc_projects/atm_project/files_folder/convert_files/{filename}', index=False) 
#         end = time.time()
#         total = end - start
#         print(total)       
#     except Exception as e:
#         print(f'Error : {e}')
#         print('Sorry For Inconvinence !!! ')  #The Program Is Going To Rerun)

# load_data()                    
                