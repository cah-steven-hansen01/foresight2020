import pyodbc
import pandas as pd
import os
import itertools
import re
import time
import datetime as dt
import sys
sys.path.insert(1, r'C:\Users\steven.hansen01\Documents\GitHub\quality_productivity_tool')
from qa_productivity_tool import quality_report, nc_full, nc_product_by_dispo



def test_pull():
    dr = Data_Retrevial()
    dr.pull_plantstar()

protocol = test_pull

class Data_Retrevial:
    def __init__(self):
        self.qr = quality_report.Quality_Report() #provides reference mappings (i.e. product code -> value stream)
    def pull_plantstar(self,start_date = '2019-01-01',filename = 'plant_star' ):
        '''pulls plantstar data, start_date must be in format "YYYY-MM-DD"'''
        s_time = time.time()
        ps_cnxn = pyodbc.connect("Provider=MSDASQL.1;Persist Security Info=True;Extended Properties=&quot;DSN=mypstar;&quot;;Initial Catalog=focus2000")
        ps_cursor = ps_cnxn.cursor()
        date_query = "{ts '" + start_date + " 06:00:00'}))"
        plant_query = '''SELECT ss_hist_base_0.start_time, ss_hist_base_0.mach_name, ss_hist_base_0.gross_pieces, ss_hist_base_0.user_text_1, ss_hist_base_0.user_text_4, ss_hist_base_0.mat_formula, ss_hist_base_0.tool, ss_hist_base_0.std_shot_weight, ss_hist_base_0.act_shot_weight
                        FROM focus2000.dg_def dg_def_0, focus2000.dg_set dg_set_0, focus2000.ss_hist_base ss_hist_base_0
                        WHERE dg_def_0.mach_seq = ss_hist_base_0.mach_seq AND dg_set_0.dg_seq = dg_def_0.dg_seq AND ((dg_set_0.dg_name Like 'Team%') AND (ss_hist_base_0.start_time>'''+date_query#{ts '2019-01-01 06:00:00'}))'''
        ps_cursor.execute(plant_query)
        rows = ps_cursor.fetchall()
        df = pd.DataFrame.from_records(rows,columns = ['start_time','mach_name','gross_pieces','user_text_1','shop_order','resin','tool','std_shot_weight','act_shot_weight'])
        print('-*'*5+' Plantstar Summary Stats '+'-*'*5)
        print('Earliest entry = {}'.format(df.start_time.min()))
        print('Most Recent entry = {}'.format(df.start_time.max()))
        print('Number of entries = {}'.format(df.shape[0]))
        print('Number of Shop Orders = {}'.format(len(df.shop_order.unique())))
        df.to_csv('./raw_data/plantstar_raw_pull.csv')
        df_clean = pd.DataFrame()
        for so, frame in df.groupby(by = 'shop_order'):
            if re.match('[0-9]*6',so) is None: continue
            elif len(so) != 6: continue
            df_clean.loc[so,'tool']      = frame['tool'].iloc[0]
            df_clean.loc[so,'mach_name'] = frame['mach_name'].iloc[0]
            df_clean.loc[so,'resin']     = frame['resin'].iloc[0]
            df_clean.loc[so,'total_pieces'] = sum(frame['gross_pieces'])
            df_clean.loc[so,'start_time']= frame['start_time'].min()
            df_clean.loc[so,'end_time']  = frame['start_time'].max()
            df_clean.loc[so,'number of entries'] = len(frame)
        df_clean = df_clean.reset_index()\
                    .rename(columns = {'index':'shop_order'})\
                    .sort_values(by = 'start_time')\
                    .reset_index(drop = True)
        try:df_clean.to_csv('./clean_data/' + filename + '.csv')
        except PermissionError:
            print('PermissionError, close file and press enter')
            _ = input('Press enter after closing ',filename)
            df_clean.to_csv('./clean_data/plant_star.csv')
        print('Planstar Pull took {} seconds'.format(round(time.time()-s_time),2))
    def pull_archived_so(self,start_date = '2019-01-01',filename = 'archived_so_clean',fname_timestamp = True):
        if fname_timestamp: filename = filename+dt.date.today().strftime('%d-%b-%Y') 
        s_time = time.time()
        d_list = start_date.split('-')
        start_date = dt.date(int(d_list[0]),int(d_list[1]),int(d_list[2])).strftime('%Y%m%d') 
        cnxn = pyodbc.connect('DRIVER={Client Access ODBC Driver (32-bit)};SIGNON=1;REMARKS=1;LIBVIEW=1;DFT=1;QRYSTGLMT=-1;PKG=CLPB80F/DEFAULT(IBM),2,0,1,0,512;LANGUAGEID=ENU;DFTPKGLIB=B833CLPF;DBQ=B833CLPF;SYSTEM=NAMFG401;UserID=CLSERVICE;Password=SERVICE;')
        cursor = cnxn.cursor()
        archived_so_query = '''
        SELECT DISTINCT FMH.MORD, FMH.MPROD, FMH.MRDTE, FMH.MQISS, FMH.MAPRD, FMH.MASTS, FMH.MBOM, FMH.MOPNO, FSH.SQFIN, FMH.MQREQ, FSH.SQREQ, FSH.SOLOT
        FROM B833CLPF.FMH FMH, B833CLPF.FSH FSH
        WHERE FMH.MORD = FSH.SORD AND ((FMH.MRDTE>{}) AND (FSH.SQFIN>0))'''.format(start_date)
        cursor.execute(archived_so_query)
        rows = cursor.fetchall()
        df = pd.DataFrame.from_records(rows,columns = ['shop_order','component','date','MQREQ','Product','MQISS','bom_qty','MOPNO','finished_qty','requested_qty','SOSTS','lot_number'])       
        df = df[['shop_order','lot_number','date','Product','component','bom_qty','requested_qty','MQREQ']]
        df.to_csv(f"./raw_data/archived_so{dt.date.today().strftime('%d-%b-%Y')}.csv")
        component_category = pd.read_csv('./reference_data/Product Category.csv',encoding='windows-1252')
        self.component_dic = {}
        for component, category in zip(component_category['Component'],component_category['Category']):
            self.component_dic[component] = category
        self.not_found_components = set()
        list_of_frames = [self._component_encoder(frame) for lot, frame in df.groupby(by = 'lot_number')]
        archived_so_clean = pd.concat(list_of_frames)\
            .reset_index(drop = True)
        lot_mask = archived_so_clean['Lot Number'].str.contains('TST|TSA|TST')
        archived_so_clean = archived_so_clean[~lot_mask]
        product_mask = archived_so_clean["Product"].str.contains('RM[0-9]{3}|5551500055') # removes regrind and tips codes (see outlier investigation)
        archived_so_clean = archived_so_clean[~product_mask]
        archived_so_clean['Value Stream'] = list(map(self.qr.return_value_stream,archived_so_clean['Product']))
        archived_so_clean.to_csv('./clean_data/'+filename+'.csv')
        print('-*'*5+' Archived SO (BPCS) Summary Stats '+'-*'*5)
        print('Archived SO (BPCS) Pull took {} seconds'.format(round(time.time()-s_time),2))
    def pull_ncs(self,filename = 'ss_ncs'):
        ncs_dispo = nc_product_by_dispo.NC_Product_By_Dispo()
        ncs_dispo.mostrecentreport()
        nc_dispo_df = ncs_dispo.run_report()
        nc_f = nc_full.NC_Full()
        nc_f.mostrecentreport()
        nc_full_df = nc_f.run_report(add_dispo=False)
        cols = ['NC Number','Created Date','Discovery/Plant Area','Root Cause','Root Cause Description','Status','Value Stream']
        nc_df = nc_dispo_df.merge(nc_full_df[cols], on = "NC Number")
        nc_df.loc[:,'Created Date'] = pd.to_datetime(nc_df['Created Date'])
        try:nc_df.to_csv('./clean_data/'+filename+'.csv')
        except PermissionError:
            print('PermissionError')
            _ = input('Close {} and press enter to try again'.format(filename))
            nc_df.to_csv('./clean_data/'+filename+'.csv')
    def pull_cff_schedule(self):
        pass
    def pull_sff_schedule(self):
        pass
    def _component_encoder(self,frame):
        
        frame = frame.reset_index(drop=True)
        lot_number = frame.loc[0,'lot_number'].strip()
        product = frame.loc[0,'Product'].strip()
        first_date = pd.to_datetime(frame.date,format='%Y%m%d').min()
        last_date = pd.to_datetime(frame.date,format='%Y%m%d').max()
        num_entries = len(frame)
        shop_order = str(frame.loc[0,'shop_order'])
        req_qty = frame.loc[0,'requested_qty']
        df = pd.DataFrame()
        df.loc[lot_number,'Product'] = product
        df.loc[lot_number,"first_entry_date"] = first_date
        df.loc[lot_number,'last_entry_date'] = last_date
        df.loc[lot_number,'shop_order'] = shop_order
        df.loc[lot_number,'requested_qty'] = req_qty
        for i,component in enumerate(frame.component):
            component = component.strip()
            try:
                self.component_dic[component]
            except KeyError:
                self.not_found_components.add(component)
                continue
            df.loc[lot_number,self.component_dic[component]] = component
        return df.reset_index()\
                .rename(columns = {'index':'Lot Number'})

if __name__ == "__main__":
    protocol()