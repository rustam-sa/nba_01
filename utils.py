import pandas as pd
from openpyxl import Workbook
import tempfile
import os
import subprocess
import time

def open_dataframe_in_temp_excel(dataframe):
    """
    Opens a DataFrame in a temporary Excel workbook.
    
    Parameters:
        dataframe (pd.DataFrame): The DataFrame to write to Excel.
    
    Returns:
        None
    """
    # Create a temporary file
    temp_file = tempfile.NamedTemporaryFile(suffix=".xlsx", delete=False)
    excel_path = "C:\\Program Files\\Microsoft Office\\root\\Office16\\EXCEL.EXE"  # Adjust if needed

    try:
        # Write the DataFrame to the temporary Excel file
        dataframe.to_excel(temp_file.name, index=False, engine='openpyxl')

        print(f"Temporary Excel file created: {temp_file.name}")
        print("Opening Excel...")

        # Check if the file exists
        if not os.path.exists(temp_file.name):
            raise FileNotFoundError(f"Temporary file {temp_file.name} does not exist.")

        # Open the Excel file
        excel_process = subprocess.Popen([excel_path, temp_file.name], shell=False)
        print("Excel opened successfully.")

        # Wait for Excel to close
        excel_process.wait()
        print("Excel closed.")
    except FileNotFoundError as e:
        print(f"Error: {e}")
    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        # Delete the temporary file
        try:
            os.unlink(temp_file.name)
            print(f"Temporary file {temp_file.name} deleted.")
        except PermissionError:
            print("Could not delete the file. Ensure Excel is fully closed.")
