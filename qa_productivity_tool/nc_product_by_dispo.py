from .quality_report import Quality_Report
import pandas as pd
import warnings
warnings.filterwarnings('ignore', category=UserWarning, module='openpyxl') #silence warning about not being able to load headers and footers

def test():
    nc_dispo = NC_Product_By_Dispo()
    nc_dispo.mostrecentreport()
    df = nc_dispo.df_disp_by_nc()
    print(df.head())
protocol = test
class NC_Product_By_Dispo(Quality_Report):
    

    def __init__(self):
        super().__init__(report_name = "NC Product By Disposition Report")
        self.status_of_report = 0

    def run_report(self):
        if self.status_of_report == 1:
            return self.data
        self.r_data = pd.read_excel(open(self.path,'rb'))
        self.data = self.r_data.copy()
        for old_col, new_col in zip(self.data.columns,self.data.iloc[7]):
            self.data = self.data.rename(columns = {old_col:new_col})
        self.data = self.data.iloc[8:]
        self.data = self.data.set_index('NC Number')
        self.data = self.data.filter(like = 'NC-',axis = 0)
        self.data = self.data.reset_index()
        self.data['Product'] = list(map(self.tmpecc_remover,self.data['Product']))
        self.data['ea Cost'] = list(map(self.return_cost,self.data['Product']))
        self.data['Cost'] = self.data['ea Cost'] * self.data['Dispositioned Quantity']
        self.status_of_report = 1
        return self.data
    def disp_by_nc(self,nc):
        '''returns dataframe of nc disposition'''
        self.check_report_status()
        cols = ['NC Number', 'Initial Failure Mode','Disposition','Lot Number','Dispositioned Quantity','Product','ea Cost',"Cost"]

        return self.data[cols][self.data["NC Number"]==nc]
    def df_disp_by_nc(self):
        '''returns index compressed df to be merged with other dfs'''
        self.check_report_status()
        new_df = pd.DataFrame()
        for i, (nc,frame) in enumerate(self.data.groupby(by = 'NC Number')):
            new_df.loc[i,"NC Number"] = nc
            new_df.loc[i,'Lot Number'] = str(frame['Lot Number'].values)
            new_df.loc[i,'Product'] = str(frame['Product'].values)
            new_df.loc[i,'Dispositioned Quantity'] = str(frame['Dispositioned Quantity'].values)
            new_df.loc[i,'Disposition'] = str(frame['Disposition'].values)
            new_df.loc[i,'ea Cost'] = str(frame['ea Cost'].values)
            new_df.loc[i,'Total Cost'] = str(sum(frame['Cost'].values))
        for col in new_df.columns:
            new_df[col] = list(map(lambda x: x.strip("''[]").replace("'",""),new_df[col].astype(str)))
        return new_df
if __name__ == '__main__':
    protocol()