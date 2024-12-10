import pandas as pd
import tempfile
import os

def open_df_in_temp_excel(df):
    # Create a temporary file with .xlsx extension
    with tempfile.NamedTemporaryFile(suffix=".xlsx", delete=False) as temp_file:
        temp_file_path = temp_file.name
    
    # Save the DataFrame to the temporary Excel file
    df.to_excel(temp_file_path, index=False, engine='openpyxl')
    
    # Open the Excel file
    os.startfile(temp_file_path)

    # Print path for reference
    print(f"Temporary Excel file opened at: {temp_file_path}")