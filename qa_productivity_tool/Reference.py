import pandas as pd
import CleaningTools as ct
import openpyxl
import numpy as np
import os
'''Obsolete class???'''

cwd = os.getcwd().split('\\')
home_path = cwd[0]+'\\' + cwd[1]+'\\'+cwd[2] 
ref_path =  home_path +'\\OneDrive - Cardinal Health\\CL_Quality_Tools\\qa_productivity_tool\\'+'\\Cross_Reference_data_v0.2.xlsx'

class Reference:
    '''Maintains product code, product family, value stream,
    failure mode, root cause location, etc...
    Note: 'all_pcs_dict' is preferred method for calling product code
    attributes and should replace evaluate_pc_look_up and evaluate_lot_sizes'''
    def __init__(self):
        global ref_path
        self.ref_path = ref_path
        self.pc_pf_vs = pd.read_excel(open(self.ref_path,'rb'),sheet_name='PC_PF_VS')
        self.pc_pf_vs['Product'] = self.pc_pf_vs['Product'].astype(str).replace('-','')
        self.lookup_table = pd.read_excel(open(self.ref_path,'rb'),sheet_name = 'Look-up Table')
        self.lookup_table['Product'] = self.lookup_table['Product'].astype(str)
        self.all_pcs = pd.read_excel(open(self.ref_path,'rb'),sheet_name='All PCs')
        self.ifm_risk = pd.read_excel(open(self.ref_path,'rb'),sheet_name='Complaint FMs')
        self.risk_map = {}
        for i, failure_mode in enumerate(self.ifm_risk['FailureMode']):
            self.risk_map[failure_mode] = self.ifm_risk.loc[i,'Risk']
        self.pc_cost = pd.read_excel(open(self.ref_path,'rb'),sheet_name='Cost')
        self.cost_map = {}
        for i, pc in enumerate(self.pc_cost['Part Number']):
            self.cost_map[pc] = self.pc_cost.loc[i,'TOTAL']
        pr_codes = pd.read_excel(open(self.ref_path,'rb'),sheet_name = 'PRFailureModeZoom')\
                            .iloc[2:]\
                            .rename(columns = {'Unnamed: 1':'PR Code','Unnamed: 2':'Failure Mode'})\
                            .reset_index(drop = True)\
                            [['PR Code','Failure Mode']]
        self.pr_codes_map = {}
        for i, code in enumerate(pr_codes['PR Code']):
            self.pr_codes_map[code] = pr_codes.loc[i,'Failure Mode']
    def pr_code_to_fm(self,pr_code):
        return self.pr_codes_map[pr_code]
    def evaluate_pc(self,df,bpcs_report = 'n'):
        self.df = df
        self.df['Product'] = self.df['Product'].astype(str)
        self.df = self.df.merge(self.pc_pf_vs, on = 'Product', how = 'left')
        return self.df
    def evaluate_pc_look_up(self,df):
        df['Product'] = df['Product'].astype(str)
        return df.merge(self.lookup_table,on='Product', how = 'left')
    def evaluate_lot_sizes(self,lot):
        self.lot_sizes = pd.read_excel(open(self.ref_path,'rb'),sheet_name='Lot Sizes')
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
    def root_cause_loc(self):
        ''' returns dict with key = 'Parent Root Cause Location' and 
        values: 0 = Job Loc, 1 = Description'''
        root_cause_df = pd.read_excel(open(self.ref_path,'rb'),sheet_name='Root Cause Location')
        root_cause_dic = {}
        for i, rc in enumerate(root_cause_df['BPCS CODE']):
            job_loc = root_cause_df.loc[i,'Job Loc']
            descrip = root_cause_df.loc[i,'Description']
            root_cause_dic[rc] = [job_loc,descrip]
        return root_cause_dic
    def evaluate_uom(self,pc):
        return self.all_pcs['Per Case or 1 if UOM is ea'][self.all_pcs['Product'] == pc]
    def evaluate_pf(self,pf):
        return self.all_pcs['Product Family'][self.all_pcs['Product']==pc]
    def tmpecc_remover(self,product_col):
        def remover(self):
            try:
                return pc.split('~')[0]
            except:
                if str(pc) == "None" or str(pc) == "nan":
                    pass
                else:
                    print('Failed for ',pc)
        return list(map(remover,product_col))
    def evaluate_cmplnt_risk(self,ifm):
        risks = []
        if str(ifm) == 'nan':
            return None
        for failure_mode in ifm.split(','):
            try:
                risks.append(self.risk_map[ifm])
            except KeyError:
                risks.append(np.nan)
        if 'High' in risks:
            return 'High'
        if 'Med' in risks:
            return 'Med'
        if 'Low' in risks:
            return 'Low'
        else:
            return 'not found'
    def evaluate_cost(self,pc):
        try:
            return round(self.cost_map[pc])
        except KeyError:
            return np.nan

def test_evaluate_cost():
    ref = Reference()
    ans = ref.evaluate_cost('8989')
    print(ans)
def test_main():
    pc_pf_vs = Reference()

def test_evaluate_pc():
    from ncr_mrb import NCR_MRB as mrb
    mrb_data = mrb()
    mrb_data, _ = mrb_data.run_report()
    pc_pf_vs = Reference()
    mrb_data=pc_pf_vs.evaluate_pc(mrb_data)
    mrb_data.to_clipboard()
def test_all_pcs_dict():
    pcs_ref = Reference()
    pcs_dict = pcs_ref.all_pcs_dict()
    print(pcs_dict)
def test_risk_map():
    risk_ref = Reference()
    print(risk_ref.evaluate_cmplnt_risk('BROKEN CONNECTOR'))
def test_pr_code_to_fm():
    ref = Reference()
    print(ref.pr_code_to_fm('PR421'))

if __name__=='__main__':
    #test_main()
    #test_evaluate_pc()
    #test_all_pcs_dict()
    #test_risk_map()
    test_pr_code_to_fm()
