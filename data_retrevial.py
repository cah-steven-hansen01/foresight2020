import pyodbc
import pandas as pd
import os
import itertools
import re
import time

def test_pull_plantstar():
    data_retrevial = Data_Retrevial()
    data_retrevial.pull_plantstar()

protocol = test_pull_plantstar

class Data_Retrevial:
    def pull_plantstar(self,start_date = '2019-01-01' ):
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
        df_clean.to_csv('./clean_data/plant_star.csv')
        print('Planstar Pull took {} seconds'.format(round(time.time()-s_time),2))
    def pull_archived_so(self):
        cnxn = pyodbc.connect('DRIVER={Client Access ODBC Driver (32-bit)};SIGNON=1;REMARKS=1;LIBVIEW=1;DFT=1;QRYSTGLMT=-1;PKG=CLPB80F/DEFAULT(IBM),2,0,1,0,512;LANGUAGEID=ENU;DFTPKGLIB=B833CLPF;DBQ=B833CLPF;SYSTEM=NAMFG401;UserID=CLSERVICE;Password=SERVICE;')
        cursor = cnxn.cursor()
        archived_so_query = '''
        SELECT DISTINCT FMH.MORD, FMH.MPROD, FMH.MRDTE, FMH.MQISS, FMH.MAPRD, FMH.MASTS, FMH.MBOM, FMH.MOPNO, FSH.SQFIN, FMH.MQREQ, FSH.SQREQ, FSH.SOLOT
        FROM B833CLPF.FMH FMH, B833CLPF.FSH FSH
        WHERE FMH.MORD = FSH.SORD AND ((FMH.MRDTE>20191231) AND (FSH.SQFIN>0))'''
        cursor.execute(archived_so_query)
        rows = cursor.fetchall()
        df = pd.DataFrame.from_records(rows,columns = ['shop_order','component','date','MQREQ','product','MQISS','bom_qty','MOPNO','finished_qty','SQREQ','SOSTS','lot_number'])       
        df = df[['shop_order','lot_number','date','product','component','bom_qty','MQREQ']]
    def pull_ncs(self):
        pass
    def pull_cff_schedule(self):
        pass
    def pull_sff_schedule(self):
        pass

if __name__ == "__main__":
    protocol()