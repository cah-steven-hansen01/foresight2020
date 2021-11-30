import os
import pandas as pd
import numpy as np
import datetime as dt

cwd = os.getcwd().split('\\')
home_path = cwd[0]+'\\' + cwd[1]+'\\'+cwd[2] 
ref_path =  home_path +'\\OneDrive - Cardinal Health\\CL_Quality_Tools\\qa_productivity_tool\\'+'\\Cross_Reference_data_v0.2.xlsx'

def pf_lookup():
    qreport = Quality_Report('test')
    pc_dict = qreport.all_pcs_dict()
    print(qreport.return_product_family('40'))
def cost_lookup():
    qreport = Quality_Report('test')
    df = qreport.return_cost('40')
    print(df)
protocol = cost_lookup
class Quality_Report(object):
    '''A class for other reports to inherit from'''
    def __init__(self,report_name, to_path = './Test_Reports' ):
        print(os.getcwd())
        #self.og_data_path = og_data_path
        global ref_path
        self.ref_path = ref_path
        self.report_name = report_name
        self.to_path = to_path
        self.all_pcs_dict()
        self.cost_df = pd.read_excel(open(self.ref_path,'rb'),sheet_name='Cost')
        self.cost_df['Part Number'] = self.cost_df['Part Number'].astype(str)
        self.pc_pf_vs = pd.read_excel(open(self.ref_path,'rb'),sheet_name='PC_PF_VS')
        self.pc_pf_vs['Product'] = self.pc_pf_vs['Product'].astype(str).replace('-','')
        self.status_of_report = 0
    def meta_data(self):
        return self.r_filename,self.path,self.date
    def mostrecentreport(self,og_data_path = 'Downloads'):
        print(f'Searching for most recent {self.report_name} file ')
        if og_data_path == 'Downloads': # derives download path
            dl_path = os.getcwd().split('\\')[:3]
            self.og_data_path = dl_path[0]+'\\'+dl_path[1]+'\\'+dl_path[2]+'\\'+'Downloads'
        else:self.og_data_path = og_data_path
        print(self.og_data_path)
        entries = {}
        with os.scandir(self.og_data_path) as it:
            for entry in it:
                if entry.name.startswith(self.report_name):
                #print(entry.name)
                    entries.update({entry.name:[entry.stat().st_ctime,entry.path]})
        for key in entries:
            if entries[key] == max(entries.values()):
                timepulled = pd.Timestamp(entries[key][0], unit = 's')
                day,month,year = timepulled.day,timepulled.month,timepulled.year
                datepulled = str(month)+"/"+str(day)+"/"+str(year)
                self.r_filename = key
                self.path = entries[key][1]
                self.date=datepulled
                return self.r_filename,self.path,self.date
    def all_pcs_dict(self):
        '''returns a dictionary with keys = Product Code and 
        values: 0 = description, 1 = UOM, 2 = Product Family and 
        3 = Value Stream, 4 = Lid Type'''
        self.all_pcs_df = pd.read_excel(open(self.ref_path,'rb'),sheet_name='All PCs')
        self.pcs_dict = {}
        for i, pc in enumerate(self.all_pcs_df['Product']):
            pc = str(pc)
            descrip = self.all_pcs_df.loc[i,'Description']
            uom = self.all_pcs_df.loc[i,'Per Case or 1 if UOM is ea']
            pf = self.all_pcs_df.loc[i, 'Product Family']
            vs = self.all_pcs_df.loc[i,'Value Stream']
            lid_type = self.all_pcs_df.loc[i,'Lid Type']
            self.pcs_dict[pc] = [descrip,uom,pf,vs,lid_type]
        return self.pcs_dict
    def return_product_family(self,pc):
        pc = pc.strip('-')
        try:
            return self.pcs_dict[pc][2]
        except KeyError:
            return 'pf not found'
    def return_value_stream(self,pc):
        pc = pc.strip('-')
        try:
            return self.pcs_dict[pc][3]
        except KeyError:
            return 'VS not found'
    def return_UOM(self,pc):
        pc = pc.strip('-')
        try:
            return self.pcs_dict[pc][1]
        except KeyError:
            return 'UOM not found'
    def return_lid_type(self,pc):
        pc = pc.strip('-')
        try:
            return self.pcs_dict[pc][4]
        except KeyError:
            return 'lid type not found'
    def return_cost(self,pc):
        if len(np.round(self.cost_df['TOTAL'][self.cost_df['Part Number']==str(pc)].values,1)) == 0:
            pass
        else:
            return np.round(self.cost_df['TOTAL'][self.cost_df['Part Number']==str(pc)].values,1)[0]
    def tmpecc_remover(self,pc):
        try:
            pc = pc.strip('-')
            return pc.split('~')[0]
        except:
            return None
    def check_report_status(self):
        if self.status_of_report == 0:
            self.run_report()
    def index_compressor(self, df,name_of_index):
        new_df = pd.DataFrame()
        i = 0
        for name_of_index, frame in df.groupby(by = name_of_index):
            for col in frame.columns:

                new_df.loc[i,col] = str(frame[col].dropna().unique()).strip('[]').strip("'")
            i+=1
        return new_df
    def date_converter(self,df, col_name_list,date_format = '%d-%b-%Y'):
        
        
        for col in col_name_list:
            df[col] = df[col].fillna(0.)
            df[col]=pd.to_datetime(df[col],errors = 'coerce')
            df[col] = df[col].dt.strftime(date_format)
            #df[col] = list(map(lambda x: x.strftime(date_format),df[col] ))
        return df
    def data_converterII(self,df,col_name_list):
        for col in col_name_list:
            df.loc[:,col] = pd.to_datetime(df[col])
            df.loc[:,col] = df[col].dt.date
        return df
    def evaluate_pc(self,df,bpcs_report = 'n'):
        self.df = df
        self.df['Product'] = self.df['Product'].astype(str)
        self.df = self.df.merge(self.pc_pf_vs, on = 'Product', how = 'left')
        return self.df
if __name__ =='__main__':
    protocol()