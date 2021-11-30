import os
import pandas as pd
import openpyxl
import numpy as np
import datetime as dt
from functools import partial
from openpyxl.chart import BarChart, Series, Reference
from quality_report import Quality_Report
from QMSPRODRP_report import QMSPRODRP_report
from Reference import Reference

def test_Regulatory_Reporting_Global_Report():
    report = Regulatory_Reporting_Global_Report()
    data = report.run_report()
    data.to_clipboard()
def test_CASE_BY_CUSTOMER():
    report = CASE_BY_CUSTOMER()
    # report.mostrecentreport()
    report.missing_lids_complaints()
    report.visual_mls_by_pf().to_clipboard()
    print(report.meta_data())
def complaint_reporter():
    cl_complaints = CASE_BY_CUSTOMER()
    cl_complaints_df = cl_complaints.run_report(lot_info=True)
    reg_complaints = Regulatory_Reporting_Global_Report()
    reg_complaints_df = reg_complaints.run_report()
    cl_complaints_set= set(cl_complaints_df['Complaint Number'])
    reg_complaints_set = set(reg_complaints_df['Complaint Number'])
    cl_reportable_cmplnts = cl_complaints_set & reg_complaints_set
    cl_complaints_df['Complaint Create Date'] = pd.to_datetime(cl_complaints_df['Complaint Create Date'])
    reg_dl_date = pd.to_datetime(reg_complaints.dl_date)
    for i, complaint in enumerate(cl_complaints_df['Complaint Number']):
        if cl_complaints_df.loc[i,'Complaint Create Date'] > reg_dl_date:
            cl_complaints_df.loc[i,'Regulatory'] = "Reg Data not Recent Enough"
        elif complaint in cl_reportable_cmplnts:
            cl_complaints_df.loc[i,'Regulatory'] = "Yes"
        else:
            cl_complaints_df.loc[i,'Regulatory'] = "No"
    cl_complaints_df.to_clipboard()
    return cl_complaints_df
def in_class_cmplnt_rprt():
    cl_complaints = CASE_BY_CUSTOMER()
    cl_complaints.complaint_reporter(site = 'IL081').to_clipboard()
def test_Complaint_Open_Tasks_By_Task_Owner_V2():
    cmplnt_tasks = Complaint_Open_Tasks_By_Task_Owner_V2()
    cmplnt_tasks.run_report()
def split_covid_restictions():
    cmplnts = CASE_BY_CUSTOMER()
    cmplnts.mostrecentreport()
    cmplnts.run_report()
    cmplnts.visual_split(ff = 'Containers',end_date=dt.date(2021,9,30))
def split_fy():
    cmplnts = CASE_BY_CUSTOMER()
    cmplnts.mostrecentreport()
    cmplnts.run_report()
    cmplnts.visual_split(ff = 'Crystal Lake',date_i=dt.date(2018,8,1), split_date=dt.date(2019,6,30),
    middle_split_date=dt.date(2020,6,30), end_date = dt.date(2021,6,30),split_titles=['FY19','FY20','FY21'])
protocol = in_class_cmplnt_rprt


class CASE_BY_CUSTOMER(Quality_Report):
    
    def __init__(self):
        super().__init__(report_name = "Case_CAH_By_Customer_Name")
    def metrics(self):
        pass
    def run_report(self,fillnanIFM = True,lot_info = True):
        self.mostrecentreport()
        self.data = pd.read_excel(open(self.path,'rb'))
        list_of_cols_to_keep = ['Complaint Number','Event Date','Complaint Create Date',
        'Event Country','Account Number','Account Name','Product','Product Name',
        'Lot # / Work Order','Complaint Quantity','UOM','Case Closed Date','Complaint Closure Date','Product Description Summary','Description Summary','Investigation Summary',
        'Sample Available','Sample Tracking Number','Sample Received Date','Reported Failure Mode',
        'Investigated Failure Mode',
        'Root Cause','Action Plan','Justification','Sales Rep','Sales Support','Customer Service',
        'Facility']
        self.data = self.data[list_of_cols_to_keep]
        self.data['Product'] = [self.tmpecc_remover(pc) for pc in self.data['Product']]
        self.data['Product Family'] = list(map(self.return_product_family,self.data['Product']))
        self.data['Value Stream'] = list(map(self.return_value_stream,self.data['Product']))
        ref = Reference()
        self.data['Risk'] = list(map(ref.evaluate_cmplnt_risk,self.data['Investigated Failure Mode']))
        if fillnanIFM == True:
            for i, ifm in enumerate(self.data['Investigated Failure Mode']):
                if str(ifm) == 'nan':
                    projected_ifms = []
                    for rfm in self.data.loc[i,'Reported Failure Mode'].split(','):
                        projected_ifms.append(ref.pr_code_to_fm(rfm))
                    self.data.loc[i,'Investigated Failure Mode'] = str(projected_ifms).strip('[]')

        if lot_info ==  True:
            self.data = self.add_lot_info(self.data)
        self.status_of_report = 1
        return self.data
    def _open_cmplnt_age(self,closure_date,create_date,sample_date,investigate_complaint):
        if investigate_complaint:
            if str(sample_date)== 'nan':
                return (dt.datetime.today() - pd.to_datetime(create_date)).days + 1
            return (dt.datetime.today() - pd.to_datetime(sample_date)).days + 1
    def _clean_lot_number(self):
        pass

    def excel_report(self):
        pass
    def missing_lids_complaints(self):
        if self.status_of_report == 0:
            self.run_report()
        self.ml_cmplnts = self.data[self.data.isin({'Investigated Failure Mode':['MISSING LIDS']})['Investigated Failure Mode']].reset_index(drop=True)
        return self.ml_cmplnts
    def visual_mls_by_pf(self):
        import matplotlib.pyplot as plt
        ml_by_pf= self.ml_cmplnts.groupby(['Product']).count()
        ml_by_pf=ml_by_pf.rename(columns = {"Complaint Number":'Count'})
        plt.bar(x = ml_by_pf.index,height = ml_by_pf['Count'].sort_values(ascending = False))
        plt.show()
        return ml_by_pf['Count']
    def visual_split(self,date_i = dt.date(2019,1,1),split_date = dt.date(2020,2,1),middle_split_date = dt.date(2021,4,1),
    end_date = dt.date.today(),ff = 'Crystal Lake',save_to_path = './Figures',missing_lids = True, split_titles = ['Pre-Covid Restrictions','Covid Restrictions','Post-Covid Restrictions']):
        '''Creates a split to compare complaints across time (for example pre an post covid)
        missing_lids = False, removes missing lid complaints'''
        from dateutil.relativedelta import relativedelta
        import matplotlib.pyplot as plt
        if ff != 'Crystal Lake':df = self.data[self.data["Value Stream"]==ff]
        else:df = self.data
        if missing_lids == False: df = self.data[(self.data['Investigated Failure Mode']!='MISSING LIDS')]
        pre_split = {}
        middle_split = {}
        post_split = {}
        df["month"] = pd.to_datetime(df['Complaint Create Date']).dt.month
        df["year"] = pd.to_datetime(df['Complaint Create Date']).dt.year
        while date_i <= end_date:
            num_opened = sum((df['month']==date_i.month) & (df['year']==date_i.year))
            if date_i > end_date:
                break
            elif (date_i <= split_date):
                pre_split[str(date_i.month)+'-'+str(date_i.year)] = num_opened
            elif (date_i <= middle_split_date):
                middle_split[str(date_i.month)+'-'+str(date_i.year)] = num_opened
            else:
                post_split[str(date_i.month)+'-'+str(date_i.year)] = num_opened
            date_i = date_i+relativedelta(months=1)
        f, (axis) = plt.subplots(nrows=1,ncols=3,sharey=True,figsize = (20,10))
        def avg_sigma_lines(values_dict):
            avg = sum(list(values_dict.values()))/len(list(values_dict.values()))
            avg_line = [avg]*(len(list(values_dict.values())))
            std = np.std(list(values_dict.values()))
            sigma2= (2*std) + avg
            sigma2_line = [sigma2] * len(list(values_dict.values()))
            sigma3 = (3*std) + avg
            sigma3_line = [sigma3] * len(list(values_dict.values()))
            return avg_line,sigma2_line,sigma3_line
        pre_avg_line,pre_sigma2_line,pre_sigma3_line = avg_sigma_lines(pre_split)
        mid_avg_line,mid_sigma2_line,mid_sigma3_line = avg_sigma_lines(middle_split)
        post_avg_line,post_sigma2_line,post_sigma3_line = avg_sigma_lines(post_split)
        ylim = max(pre_sigma3_line[0],mid_sigma3_line[0],post_sigma3_line[0])+2
        
        axis[0].plot(list(pre_split.keys()),list(pre_split.values()))
        axis[0].plot(list(pre_split.keys()),pre_avg_line)
        axis[0].plot(list(pre_split.keys()),pre_sigma2_line,label = '2 Sigma')
        axis[0].plot(list(pre_split.keys()),pre_sigma3_line,label = '3 Sigma')
        axis[0].set_title(split_titles[0])
        axis[0].tick_params(labelrotation = 30)
        axis[0].set_ylim(0,ylim)
        
        axis[1].plot(list(middle_split.keys()),list(middle_split.values()))
        axis[1].plot(list(middle_split.keys()),mid_avg_line,label = "Average")
        axis[1].plot(list(middle_split.keys()),mid_sigma2_line)
        axis[1].plot(list(middle_split.keys()),mid_sigma3_line)
        axis[1].set_title(split_titles[1])
        axis[1].tick_params(labelrotation = 30)

        axis[2].plot(list(post_split.keys()),list(post_split.values()))
        axis[2].plot(list(post_split.keys()),post_avg_line)
        axis[2].plot(list(post_split.keys()),post_sigma2_line)
        axis[2].plot(list(post_split.keys()),post_sigma3_line)
        axis[2].set_title(split_titles[2])
        axis[2].tick_params(labelrotation = 30)
        f.suptitle(f"Number of {ff} Complaints Opened by Month",fontsize = 'x-large')
        f.legend()
        f.savefig(save_to_path+'\\'+f"Number of {ff} Complaints Opened Split by Event")
        plt.show()
    def add_lot_info(self,df):
        lot_info = QMSPRODRP_report()
        lot_info.pull_lot_info()
        lot_info_keyword = partial(lot_info.lot_info,return_format = 'start date') 
        df['lot start date'] = list(map(lot_info_keyword,df['Lot # / Work Order'] ))
        #print('lot start date = {},complaintcreate date = {}'.format(type(df.loc[35,'lot start date']),type(df.loc[0,'Complaint Create Date'])))
        for i, lot_start_date in enumerate(df['lot start date']):
            try:
                df.loc[i,'days since production'] = (df.loc[i,'Complaint Create Date'] - lot_start_date).days
                #print(df.loc[i,'Complaint Create Date'],i)
            except TypeError:
                #print('type error at ',i)
                df.loc[i,'time since production'] = np.nan
        return df
    def complaint_reporter(self,site = "IL081"):
        self.run_report(lot_info=True)
        reg_complaints = Regulatory_Reporting_Global_Report()
        reg_complaints_df = reg_complaints.run_report()
        cl_complaints_set= set(self.data['Complaint Number'])
        reg_complaints_set = set(reg_complaints_df['Complaint Number'])
        cl_reportable_cmplnts = cl_complaints_set & reg_complaints_set
        self.data['Complaint Create Date'] = pd.to_datetime(self.data['Complaint Create Date'])
        reg_dl_date = pd.to_datetime(reg_complaints.dl_date)
        for i, complaint in enumerate(self.data['Complaint Number']):
            if self.data.loc[i,'Complaint Create Date'] > reg_dl_date:
                self.data.loc[i,'Regulatory'] = "Reg Data not Recent Enough"
            elif complaint in cl_reportable_cmplnts:
                self.data.loc[i,'Regulatory'] = "Yes"
            else:
                self.data.loc[i,'Regulatory'] = "No"
        open_tasks = Complaint_Open_Tasks_By_Task_Owner_V2()
        investigate_tasks,dhr_tasks = open_tasks.run_report()
        self.data['Investigate Complaint'] = self.data['Complaint Number'].isin(investigate_tasks['Complaint Number']) * 1
        self.data['Review Product History'] = self.data['Complaint Number'].isin(dhr_tasks['Complaint Number']) * 1
        self.data['Open Age'] = list(map(self._open_cmplnt_age,self.data['Complaint Closure Date'],
                                        self.data['Complaint Create Date'],self.data['Sample Received Date'],
                                        self.data['Investigate Complaint']))
        self.data['Month-Year'] = list(map(lambda x: pd.to_datetime(x).strftime('%Y-%m'),self.data['Complaint Create Date'] ))
        cols_to_keep = ['Complaint Number','Event Date','Complaint Create Date','Month-Year','Event Country','Account Name','Product',
                        'Lot # / Work Order', 'Complaint Quantity','Case Closed Date','Complaint Closure Date','Product Description Summary',
                        'Description Summary','Investigation Summary','Sample Available','Sample Tracking Number','Sample Received Date','Reported Failure Mode',
                        'Investigated Failure Mode','Root Cause','Action Plan','Justification','Sales Rep','Facility','Open Age','Product Family',
                        'Value Stream','lot start date','days since production','Risk','Regulatory','Investigate Complaint','Review Product History']
        if site != "IL081":
            cols_to_keep = ['Complaint Number','Event Date','Complaint Create Date','Month-Year','Event Country','Account Name','Product',
                        'Lot # / Work Order', 'Complaint Quantity','Case Closed Date','Complaint Closure Date','Product Description Summary',
                        'Description Summary','Investigation Summary','Sample Available','Sample Tracking Number','Sample Received Date','Reported Failure Mode',
                        'Investigated Failure Mode','Root Cause','Action Plan','Justification','Sales Rep','Facility','Open Age','Product Family',
                        'Value Stream','Risk','Regulatory','Investigate Complaint','Review Product History']
        self.data = self.data[cols_to_keep]\
            .sort_values(by = 'Complaint Create Date')\
            .reset_index(drop=True)
        return self.data
class Regulatory_Reporting_Global_Report(Quality_Report):
    def __init__(self):
        super().__init__(report_name = "Regulatory Reporting Global Report")
    def run_report(self):
        self.r_filename,self.path,self.dl_date = self.mostrecentreport()
        self.data = pd.read_excel(open(self.path,'rb'))
        self.data = self.data.iloc[7:,0]
        self.data = self.data.reset_index()
        self.data = self.data.drop('index',axis = 1)
        self.data = self.data.rename(columns = {self.data.columns[0]:"Complaint Number"})
        self.data = self.data.dropna()
        self.data["Complaint Number"] = list(map(lambda x:x[0:20],self.data['Complaint Number']))
        return self.data
class Complaint_Average_Time_to_Close(Quality_Report):
    def __init__(self):
        super().__init__(report_name= 'Complaint Average Time to Close')
    def run_report(self):
        self.mostrecentreport()
        self.data = pd.read_excel(open(self.path,'rb'))
class Complaint_Open_Tasks_By_Task_Owner_V2(Quality_Report):
    def __init__(self):
        super().__init__(report_name= 'Complaint Open Tasks By Task Owner_V2')
    def run_report(self):
        self.mostrecentreport()
        self.data = pd.read_excel(open(self.path,'rb')).iloc[:,0:6]
        for i in range(6):
            self.data = self.data.rename(columns = {list(self.data.columns)[i]:self.data.iloc[3,i]})
        self.data = self.data.rename(columns = {'Record Number':'Complaint Number'})
        self.data = self.data.iloc[4:].reset_index(drop = True)
        investigate_tasks = self.data[self.data['Task Name'] == 'Investigate Complaint']
        dhr_tasks = self.data[self.data['Task Name']=='Review Product History']
        return investigate_tasks,dhr_tasks


if __name__ == '__main__':
    protocol()