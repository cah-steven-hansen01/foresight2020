import pandas as pd
import os
import time
import datetime as dt
import numpy as np
import matplotlib.pyplot as plt
import CleaningTools as ct
import Reference
from quality_report import Quality_Report


## Test Functions ###
def test_default_report():
    report = QMSPRODRP_report()
    report.default_report(filename = 'test_QMSPRODRP_report_3.1.20thru3.1.21.xlsx',openfile = 'y')
def test_cff_pc_trans_rate():
    report = QMSPRODRP_report()
    report.run_report()
    report._cff_pc_trans_rate()
def test_deluxe_report():
    report = QMSPRODRP_report()
    report.deluxe_report(filename = 'QMSPRODRP_report(deluxe)_01-Jan-21_26-May-21.xlsx',openfile = 'y')
def test_visual_ea_per_day_distb():
    report = QMSPRODRP_report()
    report.visual_ea_per_day_distb()
def test_lot_info():
    report = QMSPRODRP_report()
    report.pull_lot_info()
    lot_info = report.lot_info('21C30863X')
    print(lot_info)


## main functions ##
def main():
    report = QMSPRODRP_report()
    report.run_report()

protocol = test_lot_info

class QMSPRODRP_report(Quality_Report):
    def __init__(self):
        super().__init__(report_name= "QMSPRODRP")
    
    def run_report(self):
        self.mostrecentreport()
        self.r_data = pd.read_excel(open(self.path,'rb'))
        self.data = self.r_data.copy()
        self.data = self.data.drop(columns = ['Unnamed: 0'])
        old_cols = self.data.columns
        new_cols = ['Trn Typ','Trans Date','Shop Order Number','Item Type','Item Cls','Product','Item Description','Warehouse','Location','Lot Number','Transaction Qty','Stk UOM','PO UOM']
        for i, c in enumerate(old_cols):
            self.data = self.data.rename(columns = {old_cols[i]:new_cols[i]})
        self.data['Product Family'] = list(map(self.return_product_family,self.data['Product']))
        self.data['Value Stream'] = list(map(self.return_value_stream,self.data['Product']))
        self.data['Per Case or 1 if UOM is ea'] = list(map(self.return_UOM,self.data['Product']))
        self.containers_samples()
        self.status_of_report = 1
        entries = {}
    def pull_lot_info(self):
        entries = {}
        local_dir = os.path.dirname(__file__)
        rel_path = 'Production_data'
        abs_file_path = os.path.join(local_dir,rel_path)
        data_folder = abs_file_path
        with os.scandir(data_folder) as folder:
            for entry in folder:
                if entry.name.startswith('QMSPRODRP'):
                    entries.update({entry.name:[entry.stat().st_ctime,entry.path]})
        dfs = [pd.read_excel(open(entries[key][1],'rb')) for key in entries]
        dfs = [self._qmsprodrp_col_cleanup(df) for df in dfs]
        df_lots = pd.concat(dfs).dropna(subset=['Lot Number'])
        self.df_lots = df_lots
        return self.df_lots
    def lot_info(self,lot_number,return_format = 'dict'):
        '''returns the start date, end date, number of days run,
        total in cases and eaches produced, product family, product code'''
        if lot_number == np.nan:
            return None
        import re
        pattern = '(:?[0-9]{2}[A-Ma-m]{1}[Oo0-9]{3}63[Xx]|[0-9]{2}[A-Ma-m]{1}[Oo0-9]{3}63)'
        try:
            lot_number = re.findall(pattern,lot_number)[0]
        except IndexError:
            if return_format=='df':
                return pd.DataFrame()
            return np.nan #"non-CL lot format"
        except TypeError:
            if return_format=='df':
                return pd.DataFrame()
            return np.nan #"non-CL lot format"
        df = self.df_lots[self.df_lots['Lot Number']==lot_number].reset_index()
        if len(df)==0:
            return np.nan #"lot not found in db"
        df.loc[:,'Trans Date'] = pd.to_datetime(df['Trans Date'],format='%m/%d/%Y')
        try:
            end_date = max(df['Trans Date'])
            start_date = min(df['Trans Date'])
        except ValueError:
            end_date = dt.datetime(year = 2000,month=1,day=1)
            start_date = dt.datetime(year = 2000,month=1,day=1)

        num_days_run = len(df)
        total_cs = int(sum(df['Transaction Qty']))
        product_code = df.loc[0,'Product']
        product_family = self.return_product_family(df.loc[0,'Product'])
        shop_order = df.loc[0,'Shop Order Number']
        df.loc[:,"Per Case or 1 if UOM is ea"] = list(map(self.return_UOM,df['Product']))
        df.loc[:,'Transaction Eaches'] = df["Per Case or 1 if UOM is ea"] * df['Transaction Qty']
        df.reset_index(drop=True)
        total_ea = int(sum(df['Transaction Eaches']))
        if return_format == 'dict':
            #return start_date
            return {"start date":start_date,
            'end date':end_date,
            'number of days run':num_days_run,
            'total in cases': total_cs,
            'total in eaches':total_ea,
            'lot number':lot_number,
            'product':product_code,
            'product family':product_family,
            'shop order':shop_order}
        elif return_format == 'start date':
            return start_date
        else:
            return df
        
    def _qmsprodrp_col_cleanup(self,df):
        df = df.drop(columns = ['Unnamed: 0'])
        old_cols = df.columns
        new_cols = ['Trn Typ','Trans Date','Shop Order Number','Item Type','Item Cls','Product','Item Description','Warehouse','Location','Lot Number','Transaction Qty','Stk UOM','PO UOM']
        for i, c in enumerate(old_cols):
            df = df.rename(columns = {old_cols[i]:new_cols[i]})
        return df

    def syringe_samples(self):
        self.syringe_df = self.data[self.data["Value Stream"]=='Syringe']
        pass
    def containers_samples(self):
        self.container_df = self.data[self.data['Value Stream'] == 'Containers'].reset_index(drop=True)
        self.clean_cff_df = pd.DataFrame()
        for i, shop_order in enumerate(self.container_df['Shop Order Number'].unique()):
            self.clean_cff_df.loc[i,'First Trans Date'] = min(self.container_df['Trans Date'][self.container_df['Shop Order Number']==shop_order].unique())
            self.clean_cff_df.loc[i,'Last Trans Date'] = max(self.container_df['Trans Date'][self.container_df['Shop Order Number']==shop_order].unique())
            self.clean_cff_df.loc[i,'Shop Order Number'] = shop_order
            self.clean_cff_df.loc[i,'Lot Number'] = self.container_df['Lot Number'][self.container_df['Shop Order Number']==shop_order].unique()
            self.clean_cff_df.loc[i,'Product'] = self.container_df['Product'][self.container_df['Shop Order Number']==shop_order].unique()
            self.clean_cff_df.loc[i,'Days Run (number of trans)'] = len(self.container_df['Shop Order Number'][self.container_df['Shop Order Number']==shop_order])
            self.clean_cff_df.loc[i,'Total Trans Qty'] = sum(self.container_df['Transaction Qty'][self.container_df['Shop Order Number'] == shop_order])
            self.clean_cff_df.loc[i,'Trans Unit'] = self.container_df['Stk UOM'][self.container_df['Shop Order Number']==shop_order].unique()
            self.clean_cff_df.loc[i,"Per Case"] = self.container_df['Per Case or 1 if UOM is ea'][self.container_df['Shop Order Number']==shop_order].unique()
            if str(self.clean_cff_df.loc[i,"Per Case"]) == 'nan':
                self.clean_cff_df.loc[i,"Per Case"] = 1
            if int(self.clean_cff_df.loc[i,"Per Case"]) == 1:
                 self.clean_cff_df.loc[i,'Total Qty (ea)'] = self.clean_cff_df.loc[i,'Total Trans Qty'] 
            else:
                self.clean_cff_df.loc[i,'Total Qty (ea)'] = self.clean_cff_df.loc[i,'Total Trans Qty']*self.clean_cff_df.loc[i,'Per Case']
            self.clean_cff_df.loc[i,'Eaches Per Trans'] = self.clean_cff_df.loc[i,'Total Qty (ea)']/self.clean_cff_df.loc[i,'Days Run (number of trans)']        
    def _cff_pc_trans_rate(self):
        ''' Used in deluxe report. Calculates the product rate of all 
        container codes and creates a separate df for it'''
        self.cff_pc_trans_rate_df = pd.DataFrame()
        for i, pc in enumerate(self.clean_cff_df['Product'].unique()):
            self.cff_pc_trans_rate_df.loc[i,'Product'] = pc
            lot_list = [lot for lot in self.clean_cff_df['Lot Number'][self.clean_cff_df['Product']==pc]]
            if np.nan in lot_list:
                self.cff_pc_trans_rate_df.loc[i,'Lots'] = ''
            else:
                 self.cff_pc_trans_rate_df.loc[i,'Lots'] = str(lot_list)
            self.cff_pc_trans_rate_df.loc[i,'Number of Lots'] = len(lot_list)
            ea_per_trans_list = [ea for ea in self.clean_cff_df['Eaches Per Trans'][self.clean_cff_df['Product']==pc]]
            self.cff_pc_trans_rate_df.loc[i, 'Average eaches/trans'] = np.round(sum(ea_per_trans_list)/len(ea_per_trans_list),2)
    def default_report(self,to_path,filename,openfile = 'n'):
        self.filename = filename
        self.to_path = to_path
        if self.status_of_report == 0:
            self.run_report()
        with pd.ExcelWriter(self.to_path + self.filename) as writer:
            self.r_data.to_excel(writer,sheet_name = 'Original Data',index = False)
            self.data.to_excel(writer,sheet_name = 'Clean Data',index = False)
            self.clean_cff_df.to_excel(writer,sheet_name = 'Containers',index = False)
        if openfile =='y':
            os.startfile(to_path+self.filename)
    def deluxe_report(self,to_path,filename,openfile = 'n'):
        self.filename = filename
        self.to_path = to_path
        if self.status_of_report == 0:
            self.run_report()
            self._cff_pc_trans_rate()
        with pd.ExcelWriter(self.to_path + self.filename) as writer:
            self.r_data.to_excel(writer,sheet_name = 'Original Data',index = False)
            self.data.to_excel(writer,sheet_name = 'Clean Data',index = False)
            self.clean_cff_df.to_excel(writer,sheet_name = 'Containers',index = False)
            self.cff_pc_trans_rate_df.to_excel(writer,sheet_name = 'CFF Prod Rates',index = False)
        if openfile =='y':
            os.startfile(to_path+self.filename)
    def visual_ea_per_day_distb(self, save_to_path = './Figures'):
        if self.status_of_report == 0:
            self.run_report()
        f, (axes) = plt.subplots(ncols=1,nrows=1,figsize = (10, 5),sharey = True)
        bins = [0,1200,3200,10000,35000,150000]
        bin_color = ['b','g','yellow','r','grey']
        N,bins, patches = axes.hist(x = self.clean_cff_df['Eaches Per Trans'],bins = bins,rwidth = .9)
        for bin_c,thispatch in zip(bin_color,patches):
            thispatch.set_facecolor(bin_c)
        
        axes.set_title('Eaches per day distribution, based on PR transactions in QMSPRODRP report.')
        axes.set_xticks(bins)
        plt.xlabel('Lot Size (log)')
        plt.ylabel('Frequency')
        axes.set_xscale('log')
        f.tight_layout()
        f.savefig(save_to_path+r'\\'+'Eaches per day distribution')
        plt.close(f)
    def clipboard_data(self, type_of_data = 'clean'):
        if self.status_of_report== 0:
            self.run_report()
        if type_of_data == 'clean':
            self.data.to_clipboard()
            print('Check Clipboard!')
        if (type_of_data == 'original') or (type_of_data == 'raw'):
            self.r_data.to_clipboard()
    



if __name__ == '__main__':
    protocol()

