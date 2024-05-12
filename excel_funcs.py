import pandas as pd 


def save_as_excel_workbook(dataframes, file_name):
    writer = pd.ExcelWriter(f'{file_name}.xlsx', engine='openpyxl')
    for i in range(len(dataframes)):
        dataframes[i].to_excel(writer, sheet_name=f'Sheet{i + 1}')
    writer.close()