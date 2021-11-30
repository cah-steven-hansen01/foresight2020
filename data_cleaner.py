import pandas as pd
import re
import os
from qa_productivity_tool import nc_full

def test():
    data_cleaner= Data_Cleaner()
    data_cleaner.pull_nc()
    data_cleaner.open_pipe()
    data_cleaner.pull_arch_so()
    data_cleaner.pull_plantstar()
    data_cleaner.pull_current_so()

protocol = test

class Data_Cleaner:
    def __init__(self):
        self.bpcs_col_key = {'MORD':"Shop Order",'MPROD':"Component",'MRDTE':'MRdate','MAPRD':"Product",'MBOM':'BOM qty',
                'SQFIN':'Finished qty','SQREQ':'Requested qty','SOLOT':'Lot Number'}
    def open_pipe(self,path = './raw_data/bpcs_plantstar_query_II.xlsx'):
        try:
            self.dfs = pd.read_excel(path,sheet_name=['archived SO','Current SO','Plantstar'])
            print('Succesfully loaded the following sheets: ')
            for name,df in self.dfs.items():
                print(f'{name}, shape = {df.shape}')
        except:
            print("Unable to load ")

    def pull_arch_so(self):
        print('cleaning archived SO data')
        cols = ['MORD','MPROD','MRDTE','MQISS','MAPRD','MBOM','MOPNO','SQFIN','MQREQ','SQREQ','SOLOT','Date']
        df = self.dfs['archived SO'][cols]
        type(df)
        df = df.rename(columns = self.bpcs_col_key)
        print('Checking lot format on archived SOs')
        df['Lot Format Match'] = list(map(self._lot_checker,df['Lot Number']))
        df = df[df['Lot Format Match'] == 1]\
            .drop(columns = 'Lot Format Match')\
                .reset_index(drop = True)
        df['Lot Number'] = df['Lot Number'].astype(str)
        df['Lot Number'] = list(map(lambda x:x.upper(),df['Lot Number']))
        print(f'archived shop order data shape = {df.shape}')
        df.to_csv(os.getcwd()+r'\\clean_data'+r'\\archived_so.csv')
        return df
    def pull_current_so(self):
        print('cleaning Current SO')
        cols = ['MORD','MPROD','MRDTE','MQISS','MAPRD','MBOM','MBOM','MOPNO','SQFIN','MQREQ','SQREQ','SOLOT','SOSTS','Date']
        df = self.dfs['Current SO'][cols]\
                .rename(columns = self.bpcs_col_key)
        df = df.drop_duplicates()\
            .reset_index(drop=True)
        df['Lot Number'] = df['Lot Number'].astype(str)
        df['Lot Number'] = list(map(lambda x:x.upper(),df['Lot Number']))
        print(f'Current SO clean shape = {df.shape}')
        df.to_csv(os.getcwd()+r'\\clean_data'+r'\\current_so.csv')
        return df
    def pull_plantstar(self):
        print('cleaning Plantstar')
        cols = ['user_text_4','start_time','mach_name','gross_pieces','user_text_1','mat_formula','tool',
                'std_shot_weight','act_shot_weight','start_time2']
        col_key = {'user_text_4':'Shop Order'}
        df = self.dfs['Plantstar'][cols]\
                .rename(columns = col_key)
        df['Shop Order'] = df['Shop Order'].astype(str)
        df = df[df['Shop Order'].map(len) == 6].reset_index(drop=True)
        print(f'Plantstar clean shape = {df.shape}')
        df.to_csv(os.getcwd()+r'\\clean_data'+r'\\plantstar.csv')
        return df
    def pull_nc(self):
        print('cleaning NC data')
        ncs = nc_full.NC_Full()
        ncs.mostrecentreport()
        df = pd.read_excel(open(ncs.path,'rb'))
        df = df.iloc[4:]
        df = df.rename(columns = df.iloc[0,:])\
                    .drop(4)\
                    .reset_index(drop=True)\
                    .dropna(subset=['Product','Lot Number'])
        
        df['Product'] = list(map(lambda x: x.split('~')[0],df['Product']))
        df['Lot Format Match'] = list(map(self._lot_checker,df['Lot Number']))
        df = df[df['Lot Format Match'] == 1].drop(columns = 'Lot Format Match')
        cols = ['NC Number','Created Date','NC Type','Discovery/Plant Area','Discovery/Plant Area Name',
                'Product','Initial Failure Mode','Lot Number','Closed Date']
        df = df[cols]
        df['Created Date'] = pd.to_datetime(df['Created Date'],format = '%m/%d/Y')
        df['Closed Date'] = pd.to_datetime(df['Closed Date'],format = '%m/%d/Y')
        df['NC TAT'] = (df['Closed Date'] - df['Created Date']).dt.days
        print(f"unique Lot Numbers with NCs = {len(df['Lot Number'].unique())}")
        df = df.reset_index(drop = True)
        df['Lot Number'] = df['Lot Number'].astype(str)
        df['Lot Number'] = list(map(lambda x:x.upper(),df['Lot Number']))
        print(f'NC data shape = {df.shape}')
        df.to_csv(os.getcwd()+r'\\clean_data'+r'\\ncs.csv')
        return df

    def _lot_checker(self,lot_number):
        '''checks if lot number matches YYMXXX63 format, 
            returns True or False
            use example: 
                df['Lot Format Match'] = list(map(self,_lot_checker,df['Lot Number']))
                df = df[df['Lot Format Match'] == 1].drop(columns = 'Lot Format Match')
        '''
        if str(lot_number) == 'nan':
            return False
        return (re.compile('(:?[0-9]{2}[A-Ma-m]{1}[Oo0-9]{3}63[Xx]|[0-9]{2}[A-Ma-m]{1}[Oo0-9]{3}63)').match(lot_number) != None) *1
        

        

if __name__ == '__main__':
    protocol()