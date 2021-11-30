'''obsolete class'''

import pandas as pd
import datetime as dt
from datetime import datetime, timedelta
import matplotlib.pyplot as plt
plt.style.use('ggplot')
['Solarize_Light2', '_classic_test_patch', 'bmh', 'classic', 'dark_background', 'fast', 'fivethirtyeight', 'ggplot', 'grayscale', 'seaborn', 'seaborn-bright', 
'seaborn-colorblind', 'seaborn-dark', 'seaborn-dark-palette', 'seaborn-darkgrid', 'seaborn-deep', 'seaborn-muted', 'seaborn-notebook', 'seaborn-paper', 'seaborn-pastel', 'seaborn-poster', 'seaborn-talk', 'seaborn-ticks', 'seaborn-white', 'seaborn-whitegrid', 'tableau-colorblind10']

def test_main():
    log = Open_NC_TS()
    log = log.update_log(14,16.28571,49.62445)
    log.to_clipboard()
def test_visual_open_nc_ts():
    log = Open_NC_TS()
    log.visual_open_nc_ts(startdate='3/1/2021')
def test_visual_waterfall():
    log = Open_NC_TS()
    log.visual_waterfall()
protocol = test_visual_open_nc_ts

class Open_NC_TS:
    ''' 1) Manages the OpenNC_ts_data.xlsx database
        2) visual_waterfall method outputs the waterfall visualization to a given 
        path'''
    def __init__(self,database_path = r'C:\\Users\steven.hansen01\data_automation\\venv\\OpenNC_ts_data.xlsx'):
        ''' read file and output information that might be useful such as,
        the last date the file was updated'''
        self.database_path = database_path
        self.ts_data = pd.read_excel(open(self.database_path,'rb'))
        self.last_update = pd.to_datetime(self.ts_data.iloc[-1,0])
        print('Log last updated: ',self.last_update)
        self.today = datetime.now()
        self.days_since_last_update = (self.today - self.last_update).days
        print("Days since last update: ", self.days_since_last_update)
        
    def update_log(self,num_of_open,thirtyTAT,included_nc_TAT):
        '''update log and then return df of log'''
        if self.days_since_last_update == 0:
            self.new_date = pd.to_datetime(self.today)
        else:
            for i in range(1,self.days_since_last_update+1):
                self.new_date = (self.last_update + timedelta(days = i))#.strftime('%m-%d-%Y')
                print(self.new_date )
                df = pd.DataFrame({
                'Date':[self.new_date],
                'Open NCs':[""],
                'Monthly TAT':[""],
                'Yearly TAT':[""]
            })
                self.ts_data = self.ts_data.append(df,ignore_index = True)

        self.ts_data['Open NCs'][self.ts_data['Date']==self.new_date ] = num_of_open
        self.ts_data['Monthly TAT'][self.ts_data['Date']==self.new_date] = thirtyTAT
        self.ts_data['Yearly TAT'][self.ts_data['Date']==self.new_date] = included_nc_TAT
        #self.ts_data['Date'] = pd.to_datetime(self.ts_data["Date"])
        
        self.change_ts_data = self.ts_data.dropna(subset = ['Open NCs'])
        self.change_ts_data = self.change_ts_data[['Date', 'Open NCs','Change in NCs']]
        self.change_ts_data  = self.change_ts_data.reset_index(drop=True)
        for i, open_ncs in enumerate(self.change_ts_data['Open NCs']):
            if i == 0:
                pass
            elif i>0:
                try:
                    self.change_ts_data.loc[i,'Change in NCs'] = float(open_ncs) - float(self.change_ts_data.loc[i-1,'Open NCs'])
                except:
                    print("issue subtracting", open_ncs,self.change_ts_data.loc[i-1,'Open NCs'] )
                
        self.change_ts_data.to_clipboard()
        self.ts_data = self.ts_data[['Date','Open NCs','Monthly TAT','Yearly TAT']]
        self.ts_data = self.ts_data.merge(self.change_ts_data, how = 'outer', on = ['Date','Open NCs'])
        with pd.ExcelWriter(self.database_path) as writer:
            self.ts_data.to_excel(writer, 'TimeSeries Data', index = False)
        return self.ts_data
    def visual_waterfall(self,save_to_path = "./Figures",days_going_back = 14):
        # setting up the data

        #plot_data = self.ts_data.dropna().tail(days_going_back)
        plot_data = self.ts_data.dropna()
        date_days_ago = dt.datetime.now()- dt.timedelta(days = days_going_back) 
        plot_data = plot_data[plot_data['Date']>date_days_ago]
        plot_data = plot_data.sort_values('Date')
        plot_data.to_clipboard()
        net = sum(plot_data['Change in NCs'].dropna())
        if net <0:
            net_color = 'g'
        else:
            net_color = 'r'
        x = plot_data['Date'].dt.strftime('%m-%d')
        y = plot_data['Change in NCs']
        colors = []
        for i in y:
            if i>0:
                colors.append('r')
            if i<=0:
                colors.append('g')
        f, (axes) = plt.subplots(ncols=2,nrows=1,figsize = (10, 5),sharey = True,
        gridspec_kw={'width_ratios': [1, .05]})
        axes[0].bar(x,y,color = colors)
        axes[0].axhline(0,linestyle='-',color = 'k')
        #axes[0].set_xlabel(rotation = 70)
        axes[0].tick_params(labelrotation=45)
        axes[1].bar(x = ['Net Change'],height = net,color = net_color)
        axes[1].axhline(0,linestyle='-',color = 'k')
        f.suptitle("Change in Open NCs Last "+str(days_going_back)+ " Days")
        f.tight_layout()
        
        #plt.show()
        f.savefig(save_to_path+r'\\'+"Waterfall")
        plt.close(f)
    def visual_open_nc_ts(self,startdate = None,save_to_path = "./Figures"):
        if startdate == None:
            plot_data = self.ts_data
        else:
            plot_data= self.ts_data[self.ts_data['Date']>startdate]
        x=plot_data['Date']
        y = plot_data['Open NCs']
        
        plt.title('Open NCs Time Series')
        plt.plot(x,y)
        plt.tight_layout()
        plt.savefig(save_to_path+r'\\'+'Open NCs Time Series')

        plt.close()


        
        pass
    
    def visual_combined(self):
        # create a single visualization from the other two
        pass
        
    
if __name__ == "__main__":
    protocol()