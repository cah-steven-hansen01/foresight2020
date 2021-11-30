import pandas as pd
import openpyxl
import datetime as dt
import time
import os
import shutil

path = r'J:\QA\~Shared\Sr. QA Reports\NC_Inv_TAT\Test'
month_year_checking = (8,2021)
list_of_closed_inv = []
for folder in os.listdir(path):
    if folder == ('NC_Inv_TAT_'+dt.date(month = month_year_checking[0], year=month_year_checking[1],day=1).strftime('%m_%Y').strip('0')):
        for sub_folder in os.listdir(path+r'\\'+folder):
            try:
                for day_folder in os.listdir(path+r'\\'+folder+r'\\'+sub_folder):
                    if day_folder != 'original_data':
                        data = pd.read_excel(path+r'\\'+folder+r'\\'+sub_folder+r'\\'+ day_folder)
                        for i, sign_off_date in enumerate(data['Inv Approval Last Sign-off Date']):
                            try:
                                if sign_off_date.month == month_year_checking[0]:
                                    list_of_closed_inv.append(data.loc[i])
                            except AttributeError:
                                pass
            except NotADirectoryError:
                closed_data = pd.read_excel(path+r'\\'+folder+r'\\'+sub_folder,sheet_name = 'NCs_closed_'+str(month_year_checking[0])+'_'+str(month_year_checking[1]))

inv_closed_tat = pd.DataFrame(list_of_closed_inv).drop_duplicates()
inv_closed_tat.merge(closed_data,how='outer',on = ['NC Number','Created Date']).drop_duplicates(subset = ['NC Number'],keep = 'last').to_clipboard()