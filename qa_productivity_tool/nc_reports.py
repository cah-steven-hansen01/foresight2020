
import pandas as pd
import openpyxl
import datetime as dt
from datetime import datetime, timedelta
import time
import os
import shutil

from quality_report import Quality_Report
from ncr_mrb import NCR_MRB as mrb
from nc_full import NC_Full
from bpcs_ncr_test import BPCS_NCR_TEST as bpcs_nc
from nc_task_phase import NC_Task_Phase
import CleaningTools as ct
import open_nc_ts



def main():
    report = NC_Reports(create_log='n')
    time_stamp = time.strftime('_%m_%d_%Y_%H%M%S',time.localtime(time.time()))
    filename = 'NC_Daily_Tracker_BI_v0_4' + time_stamp + '.xlsx'
    report.daily_tracker(filename = filename, openfile='y',foldername = 'NC_Daily_Tracker',to_path=r'J:\QA\~Shared\Sr. QA Reports\NC Task Tracker\\')
    filename = 'Inv_Tat' + time_stamp + '.xlsx'
    report.inv_tat2(filename = filename,foldername='NC_Inv_TAT',to_path=r'J:\QA\~Shared\Sr. QA Reports\NC_Inv_TAT\Test\\')

def test():
    report = NC_Reports(create_log='y')
    report.monthly_nc_cip(start_date='07/01/2021',end_date = '09/30/2021', to_path = r'J:\QA\~Shared\Reports_Metrics\1. Site Focus Factory Metrics and Review\Containers\tests',
    openfile='y')
    

protocol = main

class NC_Reports(Quality_Report):
    ''' Prepares the following NC Reports: daily NC Tracker, monthly CIP reports, 
    ad hoc reports'''
    def __init__(self,report_folder_paths=None, create_log = 'y'):
        self.create_log = create_log
        self.nc_mrb_report_status = 0
        self.nc_full_report_status = 0
        self.bpcs_report_status = 0
        #self.open_nc_time_series_path = r'C:\\Users\steven.hansen01\data_automation\\venv\\OpenNC_ts_data.xlsx'
        # paths to be inputs in the future
        if report_folder_paths==None:
            self.power_bi_path = r'J:\\QA\\~Shared\Sr. QA Reports\\NC Task Tracker\\NC_Task_Tracker_Current\\'
            self.nc_tat_folder_path = r'J:\QA\~Shared\Sr. QA Reports\NC_Inv_TAT\Test\\'
            self.teir_two_folder_path = r'J:\QA\~Shared\Sr. QA Reports\Tier 2 NC Task Tracker\Auto Reports\\'
        else:
            self.power_bi_path = report_folder_paths['PowerBi']
            self.nc_tat_folder_path = report_folder_paths['NC_TAT']
            self.teir_two_folder_path = report_folder_paths['TeirII']
    def daily_tracker(self,to_path, filename = 'NC_Daily_Tracker.xlsx', openfile = 'n',foldername = 'NC_Daily_Tracker'):
        ''' Creates a timestamped folder containing visualizations, 
        an excel spreadsheet and original data. Then sends a copy of the excel spreadsheet
        to the powerbi folder so it can be read by'''
        if self.create_log == 'y':
            self.foldername = foldername
            self.to_path = to_path
            self.folder_creator()
            self.save_to_path = self.created_path + r"\\"
        else:
            self.save_to_path = to_path + r"\\"
        self.filename = filename
        # upload NCR_MRB report
        self.open_nc_mrb()
        if self.create_log == 'y':
            shutil.copy2(self.mrb_r_path,self.r_sub_folder_path)
        self.daily_tracker.visual_age_distribution(save_to_path=self.save_to_path)
        self.daily_tracker.visual_NC_age_bar(save_to_path=self.save_to_path)
        self.daily_tracker.visual_tierII_tracker(save_to_path=self.teir_two_folder_path,openfile = 'n')
        self.num_of_open, self.percent_open_under_60d = self.daily_tracker.metrics()
        # upload NC_Full report
        self.open_nc_full()
        if self.create_log == 'y':
            shutil.copy2(self.full_nc_r_path,self.r_sub_folder_path)

        thirty_days_ago = dt.date.today() - dt.timedelta(days = 30)

        # meta data compiling
        self.report_meta_data = {
            'NCR MRB report created: ':self.mrb_date_pulled,
            'NCR MRB report name: ': self.mrb_filename,
            'NC Full report created: ':self.full_nc_date_pulled,
            'NC Full report name: ': self.full_nc_filename
        }
        self.report_meta_data_S = pd.Series(self.report_meta_data)
        # metrics compiling
        self.daily_tracker_metrics = {
            'Report Pulled':self.mrb_date_pulled,
            'Number of open NCs':self.num_of_open,
            'Percent open under 60 Days':'{:.1%}'.format(self.percent_open_under_60d),
            'FY22 TAT':'{:.2f}'.format(self.fy22_TAT),
            'Running 30 Day TAT':'{:.2f}'.format(self.thirtyTAT)
            }
            #'Running Yearly TAT (including old calibrations)':'{:.2f}'.format(self.yrTAT),
            #'Running Yearly TAT (excluding old calibrations)':'{:.2f}'.format(self.included_nc_TAT)
        self.daily_tracker_metrics_S = pd.Series(self.daily_tracker_metrics)

        

        # NC Tracker Excel
        with pd.ExcelWriter(self.save_to_path + self.filename) as writer:
            self.report_meta_data_S.to_excel(writer,sheet_name = 'Report Meta Data',header = False)
            self.full_tracker_df.to_excel(writer,sheet_name='nc_full',index = False)
            self.daily_tracker_df.to_excel(writer,sheet_name = 'nc_mrb',index = False)
            self.task_count_s.to_excel(writer,sheet_name = 'task_count', index = True)
            self.daily_tracker_metrics_S.to_excel(writer, sheet_name = 'daily metrics', header = False)
            self.ts_data.to_excel(writer,'TimeSeries Data',index = False)
        # NC Tracker for Power BI
        if self.create_log == 'y':
            with pd.ExcelWriter(self.power_bi_path + "NC_Daily_Tracker.xlsx") as writer:
                self.report_meta_data_S.to_excel(writer,sheet_name = 'Report Meta Data',header = False)
                self.full_tracker_df.to_excel(writer,sheet_name='nc_full',index = False)
                self.daily_tracker_df.to_excel(writer,sheet_name = 'nc_mrb',index = False)
                self.task_count_s.to_excel(writer,sheet_name = 'task_count', index = True)
                self.daily_tracker_metrics_S.to_excel(writer, sheet_name = 'daily metrics', header = False)
                self.ts_data.to_excel(writer,'TimeSeries Data',index = False)
        # open File
        if openfile == 'y':
            os.startfile(self.save_to_path+self.filename)
    def monthly_nc_cip(self,to_path,start_date,end_date,deluxe = True,filename = 'NC_Monthly_Report.xlsx',foldername = 'NC_Monthly',openfile = 'n'):
        '''deluxe = False, returns the df with less columns'''
        start_date = pd.to_datetime(start_date)
        end_date = pd.to_datetime(end_date)
        if self.create_log == 'y':
            self.foldername = foldername
            self.to_path = to_path
            self.folder_creator()
            self.save_to_path = self.created_path + r"\\"
        else:
            self.save_to_path = to_path + r"\\"
        self.filename = filename
        self.combine_ncfull_bpcs()
        self.ncfull_bpcs_df = self.ncfull_bpcs_df[(self.ncfull_bpcs_df['Created Date']>start_date) & 
        ((self.ncfull_bpcs_df['Created Date']<end_date))]
        if deluxe==False:
            skinny_cols = ['NC Number','Created By_x','Discovery/Plant Area','Product','Initial Failure Mode',
            'Lot Number','Quantity Affected','Immediate Actions','Occurrence Date','Failure Mode',
            'Root Cause','Disposition','Dispositioned Quantity','Phase','Status_x','Closed Date',
            'TAT (Days)','Product Family_x','Value Stream_x','SmartSolve NC','NCR Number','NCR Open Date',
            'NCR Closed Date','Status_y','Shop Order','Area/Line','Containment Code','NCR Qty',
            'Sample Qty','Defect Qty','Source Type Defined1','Nonconformance Category','Parent Part Number',
            'Parent Lot Number','Item Classification','Description_y','Parent Root Cause Location',
            'Product Family_y','Value Stream_y','Lid Type','RootCauseLoc']
            self.ncfull_bpcs_df = self.ncfull_bpcs_df[skinny_cols]
        if self.create_log == 'y':
            shutil.copy2(self.full_nc_r_path,self.r_sub_folder_path)
            shutil.copy2(self.bpcs_path,self.r_sub_folder_path)

        with pd.ExcelWriter(self.save_to_path + self.filename) as writer:
            self.ncfull_bpcs_df.to_excel(writer,sheet_name = "nc_full_bpcs",index=False)
        # open File
        if openfile == 'y':
            os.startfile(self.save_to_path+self.filename)

    def open_nc_mrb(self):
        self.daily_tracker = mrb()
        self.mrb_filename,self.mrb_r_path,self.mrb_date_pulled = self.daily_tracker.mostrecentreport()
        # = self.daily_tracker.meta_data()
        self.daily_tracker_df, self.task_count_s = self.daily_tracker.run_report()
        self.nc_mrb_report_status = 1
    def open_nc_full(self):
        
        self.full_tracker = NC_Full()
        self.full_nc_filename,self.full_nc_r_path,self.full_nc_date_pulled = self.full_tracker.mostrecentreport()
        self.full_tracker_df = self.full_tracker.run_report()

        self.yrTAT, self.thirtyTAT,self.fy22_TAT = self.full_tracker.metrics()

        thirty_days_ago = dt.date.today() - dt.timedelta(days = 30)

        self.full_tracker.sub_num_open_timeline(from_date=thirty_days_ago,save_to_path=self.save_to_path,fig_title = 'Number of Open NCs (last 30 days)')

        self.full_tracker.visual_waterfallish(save_to_path=self.save_to_path)
        self.ts_data = self.full_tracker.nc_hist_open_tat()
        
        date_fields = ['Created Date','Report Date','Occurrence Date','Closure Due Date','Closed Date']  
        self.full_tracker_df = self.data_converterII(self.full_tracker_df,date_fields)    # makes dates more filter friendly.
        self.nc_full_report_status = 1

    def open_bpcs(self):
        self.bpcs_nc = bpcs_nc()
        self.bpcs_filename,self.bpcs_path,self.bpcs_date_pulled = self.bpcs_nc.meta_data()
        self.bpcs_df = self.bpcs_nc.create_df_for_ss()
        self.bpcs_report_status = 1
    
    def combine_bpcs_mrb(self,filename = 'NC_combine.xlsx', openfile = 'n'):
        self.daily_tracker_df = mrb()
        self.daily_tracker_df, _= self.daily_tracker_df.run_report()
        self.bpcs_nc_df = bpcs_nc()
        self.bpcs_nc_df = self.bpcs_nc_df.run_report()
        self.bpcs_nc_df = self.bpcs_nc_df[self.bpcs_nc_df['SmartSolve NC']=='y']
        self.test_df = pd.merge(self.daily_tracker_df,self.bpcs_nc_df,how='left',on = 'NC Number')
        self.test_df.to_clipboard()
    
    def combine_ncfull_bpcs(self):
        if self.nc_full_report_status == 0:
            self.open_nc_full()
        if self.bpcs_report_status == 0:
            self.open_bpcs()
        self.ncfull_bpcs_df = pd.merge(self.full_tracker_df,self.bpcs_df,how='left',on = ['NC Number'])
    def folder_creator(self):
        time_stamp = time.strftime('_%m_%d_%Y_%H%M%S',time.localtime(time.time()))
        self.created_path = self.to_path + self.foldername + time_stamp
        os.mkdir(self.created_path)
        self.r_sub_folder_path = self.created_path + r"\\original_data"
        os.mkdir(self.r_sub_folder_path)
    def _monthly_folder_creator(self):
        '''strategy: check if month folder is there, if not create folder and output
        self.nc_tat_folder_path which is currently defined in __init___'''
        self.month_foldername = "NC_Inv_TAT_" +str(dt.datetime.now().month)+'_'+str(dt.datetime.now().year)
        self.month_created_path = self.nc_tat_folder_path+r'\\'+self.month_foldername+r'\\' 
        if self.month_foldername not in os.listdir(self.nc_tat_folder_path):
            os.mkdir(self.month_created_path)
    def inv_tat2(self,filename,foldername='NC_Inv_TAT',to_path=r'J:\QA\~Shared\Sr. QA Reports\NC_Inv_TAT\Test\\'):
        if self.create_log == 'y':
            self.foldername = foldername
            self.to_path = to_path
            # placeholder to create folder path per month
            self.folder_creator()
            self.save_to_path = self.created_path + r"\\"
        else:
            self.save_to_path = to_path + r"\\"
        self.inv_tat_filename = filename # probably doesn't need to be 'self'
        if self.nc_full_report_status == 0:
            self.open_nc_full()
        self.nc_task = NC_Task_Phase(to_path = to_path)
        self.nc_task.update_log(self.inv_tat_filename,full_tracker = self.full_tracker)


if __name__ == '__main__':
    print('NC Reports')
    protocol()
