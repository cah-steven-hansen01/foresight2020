import pandas as pd
import os
import time

import CleaningTools as ct
import Reference

def test_main():
    bpcs_ncr = BPCS_NCR_TEST()
    filename = 'BPCS_NCR' + str(time.time()).split('.')[0]+'.xlsx'
    bpcs_ncr.run_report(openfile='y',filename=filename)

def test_nc_num_finder():
    bpcs_ncr = BPCS_NCR_TEST()
    nc = 'SO#188879                                         NC-IL081-16667'
    bpcs_ncr.nc_num_finder(nc)

def test_create_df_for_ss():
    bpcs_nc_df = BPCS_NCR_TEST()
    bpcs_nc_df = bpcs_nc_df.create_df_for_ss()
    bpcs_nc_df.to_clipboard()

protocol = test_create_df_for_ss

class BPCS_NCR_TEST:
    ''' Converts BPCS output to a readable usable form'''

    def __init__(self):
        self.r_filename, self.path, self.date = ct.mostrecentreport("NCR_TEST")
        print(self.path)
        self.non_ss_nc_count = 0 # helps create index for non-SS NCs for index compression
        self.run_report_status = 0
    def meta_data(self):
        return self.r_filename, self.path, self.date 
    def run_report(self,openfile = 'n',filename = 'BPCS_NCR.xlsx',to_path = r"C:\\Users\\steven.hansen01\\data_automation\\venv\\Test_Reports\\",):
        '''1) does not create a saved file unless openfile is 'y'
        2) returns clean dataframe'''
        self.to_path = to_path
        self.filename = filename
        self.r_data = pd.read_excel(open(self.path,'rb'))
        self.c_data = self.r_data
        self._column_cleanup()
        self.c_data["NC Number"] = list(map(self.nc_num_finder,self.c_data['Shop Order']))
        for i, nc in enumerate(self.c_data['NC Number']):
            if 'NC' in nc:
                self.c_data.loc[i,'SmartSolve NC'] = 'y'
            else:
                self.c_data.loc[i,'SmartSolve NC'] = 'n'
        self.c_data = self.c_data.reindex(columns = ['NC Number','SmartSolve NC','NCR Number', 'NCR Open Date', 'NCR Closed Date',
       'NCR Cycle Time', 'Status', 'Shop Order',
       'BPCS Entry Date', 'BPCS Closure', 'Inititated By',
       'Area/Line', 'Containment Code', ' SCAR Indicator',
       'NCR Qty', 'Sample Qty', 'Defect Qty',
       'Source Type Defined1', 'Nonconformance Category',
       'Parent Part Number', 'Parent Lot Number','Item Classification',
       'Parent Unit of\nMeasure', 'Parent Failure Mode', 'Description',
       'Short Description', 'Parent Root Cause Location', 'Created By', 'Create Date'])
        
        self.ref = Reference.Reference()
        self.ref_dict = self.ref.all_pcs_dict()
        
        for i, pc in enumerate(self.c_data['Parent Part Number']):
            try:
                self.c_data.loc[i,'Product Family'] = self.ref_dict[pc][2] # 2 - product family
                self.c_data.loc[i,'Value Stream'] = self.ref_dict[pc][3] # 3 - value stream
                self.c_data.loc[i,'Lid Type'] = self.ref_dict[pc][4]
            except KeyError:
                self.c_data.loc[i,'Product Family'] = 'Not Found'
                self.c_data.loc[i,'Value Stream'] = 'Not Found'
                self.c_data.loc[i,'Lid Type'] = 'Not Found'
        self.ref_rc_loc = self.ref.root_cause_loc()
        for i, rc in enumerate(self.c_data['Parent Root Cause Location']):
            try:
                self.c_data.loc[i,'RootCauseLoc'] = self.ref_rc_loc[rc][0]
            except KeyError:
                self.c_data.loc[i,'RootCauseLoc'] = 'Not Found'
        if openfile == 'y':
            with pd.ExcelWriter(self.to_path + self.filename) as writer:
                self.c_data.to_excel(writer, sheet_name = 'Clean Data', index = False)
                self.r_data.to_excel(writer, sheet_name = 'Original Data', index = False)
            os.startfile(self.to_path+self.filename)
        else:
            return self.c_data

        self.c_data = ct.index_compiler("NC Number",self.c_data)
    
        self.run_report_status = 1
    def create_df_for_ss(self):
        '''outputs a dataframe with only hold records with valid smartsolve NCs'''
        if self.run_report_status == 0:
            self.run_report()
        return self.c_data[self.c_data['SmartSolve NC']=='y']
    def _column_cleanup(self):
        columns_to_drop = ['\n\nType','\nPerson\nResponsible','\nVendor\nNumber','\nVendor\nName','\nRework\nInstructions',
                   'Parent Item\nConfirmed Defect\nDescription','\nParent Item\nCause',
                  'Parent\nCorrection\nCorrective Action',  'Multiple\nRecord\nSeq#', 'Component\nPart\nNumber',
       '\nComponent\nUOM', '\nComponent\nFailure Mode',
       '\nComponent\nRoot Cause', 'Component\nReturn to Vendor\nQuantity',
       'Component\nRework\nQuantity', 'Component\nSort Good\nQuantity',
       'Component\nUse As Is\nQuantity', 'Component\nScrap\nQuantity',
       'Component\nSort Bad\nQuantity', 'Component\nN/A #1\nQuantity',
       'Component\nN/A #2\nQuantity', 'Component\nN/A #3\nQuantity',
       'Component\nConfirmed Defect\nDescription', '\nComponent\nCause',
       'Component\nCorrection /\nCorrective Action','Parent\nReturn to Vendor\nQuantity', 'Parent\nRework\nQuantity',
       'Parent\nSort Good\nQuantity', 'Parent\nUse As Is\nQuantity',
       'Parent\nScrap\nQuantity', 'Parent\nSort Bad\nQuantity',
       'Parent\nN/A #1\nQuantity', 'Parent\nN/A #2\nQuantity',
       'Parent\nN/A #3\nQuantity']
        self.c_data = self.c_data.drop(columns = self.c_data.columns[0])
        self.c_data = self.c_data.drop(columns =self.c_data[columns_to_drop])
        old_columns = list(self.c_data.columns)
        new_columns_names = ['NCR Number', 'NCR Open Date', 'NCR Closed Date',
       'NCR Cycle Time', 'Status', 'Shop Order',
       'BPCS Entry Date', 'BPCS Closure', 'Inititated By',
       'Area/Line', 'Containment Code', ' SCAR Indicator',
       'NCR Qty', 'Sample Qty', 'Defect Qty',
       'Source Type Defined1', 'Nonconformance Category',
       'Parent Part Number', 'Item Classification','Parent Lot Number',
       'Parent Unit of\nMeasure', 'Parent Failure Mode', 'Description',
       'Short Description', 'Parent Root Cause Location', 'Created By', 'Create Date']
        for i,c in enumerate(old_columns):
            self.c_data = self.c_data.rename(columns={old_columns[i]:new_columns_names[i]})

    def nc_num_finder(self,shop_order):
        """ shop_order is the column the NC numbers are found under in the raw BPCS report"""
        il081 = shop_order.upper().find("IL081")
        entry_len = len(shop_order)
        if il081 != -1: # -1 means it was not found
            nc_num = []
            i = il081+6
            digit_on = 0
            while i < 100: # used 100 
                if (i==entry_len):
                    nc_num.append(i+1)
                    i = 101
                elif shop_order[i].isdigit():
                    digit_on = 1
                    nc_num.append(i)
                elif (digit_on == 1) & (shop_order[i].isdigit()==False):
                    nc_num.append(i+1)
                    digit_on = 0
                    i = 101
                i+=1
            start_nc_num = min(nc_num)
            end_nc_num = max(nc_num)
            return ("NC-IL081-" + shop_order[start_nc_num:end_nc_num]).strip()
        else:
            self.non_ss_nc_count +=1
            return "No SmartSolve Number " + str(self.non_ss_nc_count)





if __name__ == '__main__':
    protocol()
    

