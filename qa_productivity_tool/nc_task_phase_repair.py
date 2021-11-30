import pandas as pd
import datetime as dt
import os
import time
import shutil

import CleaningTools as ct
from quality_report import Quality_Report

# Tests
def test_main():
    nc_task = NC_Task_Phase()
    r_data = nc_task.run_report()
    nc_task.return_nc_phase('NC-IL081-24335' ,'Investigation')
    nc_task.inv_tat_nc('NC-IL081-29829')
def test_default_report():
    nc_task = NC_Task_Phase()
    nc_task.default_report()
def test_update_log():
    import nc_full as nf
    full_tracker = nf.NC_Full()
    nc_task = NC_Task_Phase()
    nc_task.update_log('Test_file',full_tracker)
def test_monthly_folder_creator():
    nc_task = NC_Task_Phase()
    nc_task._monthly_folder_creator()
def test_combined_tab():
    import nc_full as nf
    full_tracker = nf.NC_Full()
    full_tracker.mostrecentreport()
    nc_task = NC_Task_Phase()
    combined = nc_task.update_log('Test_file',full_tracker,test = 'y')
    combined.to_clipboard()
protocol = test_combined_tab

class NC_Task_Phase(Quality_Report):
    '''Primarily used to output inv TAT, inv_tat only available for open NCs'''
    def __init__(self,to_path = None):
        super().__init__(report_name = "NC Average Time to Close TaskPhase Report")
        #self.r_filename, self.path, self.date = ct.mostrecentreport("NC Average Time to Close TaskPhase Report")
        #self.nc_tat_folder_path = r'J:\QA\~Shared\Sr. QA Reports\NC_Inv_TAT\Test\\'
        self.to_path = to_path
        #self.status_of_report = 0
    def meta_data(self):
        return self.r_filename, self.path, self.date
    def run_report(self):
        # cleaning
        self.mostrecentreport()
        self.r_data = pd.read_excel(open(self.path,'rb'))
        self.data = self.r_data.iloc[7:]
        cols = self.r_data.iloc[6,:]
        for i,col in enumerate(self.data.columns):
            self.data=self.data.rename(columns = {self.data.columns[i]:cols[i]})
        date_cols = ['Created Date','Start Date','Sign-off Date','Task Due Date']
        ct.date_converter(self.data,date_cols)
        phases = ['Adhoc Task Phase','Containment','Disposition Approval','Disposition Execution',
        'Disposition Planning','Due Date Extension Approval','Initiation Verification',
        'Investigation','Investigation Approval']
        self.data=self.data.reset_index(drop=True)
        current_phase =  'Adhoc Task Phase'
        for i, ind in enumerate(self.data['NC Number']):
            if ind in phases:
                current_phase = ind
                self.date = self.data[self.data['NC Number']!=ind]
            self.data.loc[i,'Phase'] = current_phase
        self.data = self.data.dropna(subset = ['Created Date'])
        self.data = self.data.loc[:,self.data.columns.notnull()]
        self.data['NC Task Age'] = self.data['NC Task Age'].astype('int')
        self.status_of_report = 1
        return self.data
    def update_log(self, filename,full_tracker,folder = 'NC_Inv_TAT',test = 'n'):
        '''Note: * full_tracker needs to be passed as a class 
                 * test is being used temporarily'''
        full_tracker_df = full_tracker.run_report()
        self.full_nc_filename,self.full_nc_r_path,self.full_nc_date_pulled = full_tracker.meta_data()
        # Finding NCs closed this month
        def to_month(d):
            try: return pd.to_datetime(d).month
            except: pass
        def to_year(d):
            try: return pd.to_datetime(d).year
            except: pass
        full_tracker_df["Closed Month"] = list(map(to_month,full_tracker_df['Closed Date']))
        full_tracker_df["Closed Year"] = list(map(to_year,full_tracker_df['Closed Date']))
        nc_closed_df = full_tracker_df.dropna(subset = ['Closed Date'])
        month = dt.datetime.now().month
        year = dt.datetime.now().year
        nc_closed_this_month = nc_closed_df[(nc_closed_df["Closed Month"] == month) &
        (nc_closed_df['Closed Year']==year)]
        nc_closed_this_month = nc_closed_this_month[['NC Number','Created Date','Closed Date']]
        self.run_report()

        # Look at open NCs
        open_nc_df = full_tracker_df[full_tracker_df['Status']=='INWORKS']
        open_nc_df = open_nc_df.reset_index(drop=True)
        inv_tat_dict = {}
        for i, nc in enumerate(open_nc_df['NC Number']):
            tat,last_sign_off_date = self.inv_tat_nc(nc)
            appr_tat,appr_last_sign_off_date = self.inv_approval(nc)
            created_date = open_nc_df.loc[i,'Created Date']
            inv_tat_dict[i] = [nc,created_date,tat,last_sign_off_date,appr_tat,appr_last_sign_off_date]
        inv_tat_df = pd.DataFrame.from_dict(inv_tat_dict,orient = 'index',columns = ['NC Number','Created Date','Inv TAT','Last Sign-off Date',
        'Inv Approval TAT','Inv Approval Last Sign-off Date'] )
        inv_tat_df['Total Inv TAT'] = inv_tat_df['Inv TAT'] + inv_tat_df['Inv Approval TAT']
        
        # compare open NCs with closed NCs
        combined_ncs = nc_closed_this_month.merge(inv_tat_df,on=['NC Number','Created Date'],how = 'outer')
        ''' trying to create a column that determines if the NC should be inluded, not having any luck
        line 124 is commented out so this shouldn't affect running the code'''
        def included_TAT(nc):
            current_month = dt.datetime.now().month
            line_item = combined_ncs[combined_ncs['NC Number']==nc]
            print(line_item)
            print(type(line_item))
            closed_month = pd.to_datetime(line_item["Closed Date"])[0].month
            print(closed_month)
            print(type(closed_month))
            if closed_month == current_month:
                return 'y'
            approval_month = pd.to_datetime(line_item['Inv Approval Last Sign-off Date']).month
            if approval_month == current_month:
                return 'y'
            else:
                return 'n'
        #combined_ncs['Include in TAT?'] = list(map(included_TAT,combined_ncs['NC Number']))
        if test == 'y':
            return combined_ncs
        nc_tat_month_log_filename = 'NC_TAT_Log_'+str(dt.datetime.now().month)+'_' + str(dt.datetime.now().year)+'.xlsx'
        self._monthly_folder_creator()
        self._daily_folder_creator()

        shutil.copy2(self.path,self.r_sub_folder_path)
        shutil.copy2(self.full_nc_r_path,self.r_sub_folder_path)
        # Daily 
        with pd.ExcelWriter(self.daily_created_path+r'\\'+filename+'.xlsx') as writer:
            inv_tat_df.to_excel(writer, sheet_name = 'NCs_inv_TAT',index=False)
        try:
            nc_tat_month_log = pd.read_excel(open(self.month_created_path + nc_tat_month_log_filename,'rb'))
            nc_tat_month_log.update(inv_tat_df)
            nc_tat_month_log = nc_tat_month_log.merge(inv_tat_df,how = 'outer')
            print('updating current NC TAT log')

            with pd.ExcelWriter(self.month_created_path  + nc_tat_month_log_filename) as writer:
                nc_tat_month_log.to_excel(writer,sheet_name = 'NCs_inv_TAT_open',index = False)
                nc_closed_this_month.to_excel(writer,sheet_name = 'NCs_closed'+'_'+str(dt.datetime.now().month)+'_'+str(dt.datetime.now().year),index=False)
                combined_ncs.to_excel(writer,sheet_name = 'Combined',index = False)
        except FileNotFoundError:
             print('creating new NC TAT log - Happy '+ str(dt.datetime.now().ctime()[4:7]))
             with pd.ExcelWriter(self.month_created_path+ nc_tat_month_log_filename) as writer:
                inv_tat_df.to_excel(writer,sheet_name = 'NCs_inv_TAT_open',index = False)
                nc_closed_this_month.to_excel(writer,sheet_name = 'NCs_closed'+'_'+str(dt.datetime.now().month)+'_'+str(dt.datetime.now().year),index=False)
                combined_ncs.to_excel(writer,sheet_name = 'Combined',index = False)
    def _monthly_folder_creator(self):
        self.month_foldername = "NC_Inv_TAT_" +str(dt.datetime.now().month)+'_'+str(dt.datetime.now().year)
        self.month_created_path = self.to_path+r'\\'+self.month_foldername+r'\\' 
        if self.month_foldername not in os.listdir(self.to_path):
            os.mkdir(self.month_created_path)
    def _daily_folder_creator(self):
        time_stamp = time.strftime('_%m_%d_%Y_%H%M%S',time.localtime(time.time()))
        self.daily_created_path = self.month_created_path + "NC_Inv_TAT_" + time_stamp
        os.mkdir(self.daily_created_path)
        self.r_sub_folder_path = self.daily_created_path + r'\\original_data'
        os.mkdir(self.r_sub_folder_path)
            


    def return_phase(self,phase):
        return self.data[self.data['Phase']==phase]
    def return_nc_phase(self,nc,phase):
        return_data = self.return_phase(phase)
        return_data[return_data['NC Number']==nc].to_clipboard()
        return return_data[return_data['NC Number']==nc]
    def inv_tat_nc(self,nc):
        if self.status_of_report==0:
            self.run_report()
        tat = sum(self.return_nc_phase(nc,'Investigation')['NC Task Age'])
        try: 
            last_sign_off_date = max(self.return_nc_phase(nc,'Investigation')['Sign-off Date'])
        except ValueError:
            last_sign_off_date = "No Inv sign-off"
        return tat, last_sign_off_date
    def inv_approval(self, nc):
        if self.status_of_report==0:
            self.run_report()
        approval_tat = sum(self.return_nc_phase(nc,'Investigation Approval')['NC Task Age'])
        try:
            last_sign_off_date = max(self.return_nc_phase(nc,'Investigation')['Sign-off Date'])
        except ValueError:
            last_sign_off_date = 'No Inv Approval sign-off'
        return approval_tat, last_sign_off_date
    def default_report(self):
        if self.status_of_report == 0:
            self.run_report()
        self.data.to_clipboard()  
if __name__ == "__main__":
    protocol()