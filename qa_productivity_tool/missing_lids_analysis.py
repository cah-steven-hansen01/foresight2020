import pandas as pd
import matplotlib.pyplot as plt
import datetime as dt
plt.style.use('seaborn-dark')
['Solarize_Light2', '_classic_test_patch', 'bmh', 'classic', 'dark_background', 'fast', 'fivethirtyeight', 'ggplot', 'grayscale', 'seaborn', 'seaborn-bright', 
'seaborn-colorblind', 'seaborn-dark', 'seaborn-dark-palette', 'seaborn-darkgrid', 'seaborn-deep', 'seaborn-muted', 'seaborn-notebook', 'seaborn-paper', 'seaborn-pastel', 'seaborn-poster', 'seaborn-talk', 'seaborn-ticks', 'seaborn-white', 'seaborn-whitegrid', 'tableau-colorblind10']

from case_by_customer import CASE_BY_CUSTOMER
from nc_full import NC_Full

# class Missing_Lids_Analysis(Quality_Report):
#     def __init__(self):
#         super().

ml_ncs = NC_Full()
ml_ncs.mostrecentreport()
ml_ncs = ml_ncs.missing_lid_ncs()
ml_ncs['Month-Year'] = list(map(lambda x: pd.to_datetime(x).strftime('%m-%Y'),ml_ncs['Created Date'] ))
ml_ncs.sort_values(by = 'Created Date')

ml_ncs.to_clipboard()


ml_cmplnts = CASE_BY_CUSTOMER()
ml_cmplnts.mostrecentreport()
ml_cmplnts = ml_cmplnts.missing_lids_complaints()
ml_cmplnts['Month-Year']  = pd.to_datetime(ml_cmplnts['Complaint Create Date'],format='%m-%Y')
ml_cmplnts.sort_values(by = 'Complaint Create Date')
ml_cmplnts["Month-Year"] = list(map(lambda x: pd.to_datetime(x).strftime('%m-%Y'),ml_cmplnts['Complaint Create Date'] ))
distributors = {'MEDLINE':{},'HENRY SCHEIN':{},'CARDINAL':{},'FISHER':{},'ONCOLOGY':{},'OTHER':{},'All Accounts':{}}
for i, account_name in enumerate(ml_cmplnts['Account Name']):
    account_name = account_name.upper()
    for dist_name,_ in distributors.items():
        if dist_name in account_name:
            ml_cmplnts.loc[i,'Distributor'] = dist_name 
ml_cmplnts = ml_cmplnts.fillna({'Distributor':'OTHER'})
years_ago = 2
years_ago = dt.datetime(year = dt.datetime.now().year - years_ago, \
                        month = dt.datetime.now().month, \
                        day = 1)
for i in range(24):
    month_year = (years_ago + dt.timedelta(days=31*i)).strftime('%m-%Y')
    cmplnts_that_month_year = ml_cmplnts[ml_cmplnts['Month-Year']==month_year]
    for dist_name,_ in distributors.items():
        distributors[dist_name][month_year] = cmplnts_that_month_year['Distributor'][cmplnts_that_month_year['Distributor']==dist_name].count()
    distributors['All Accounts'][month_year] = cmplnts_that_month_year['Distributor'].count()
for dist_name, val in distributors.items():
    plt.plot(val.keys(),val.values(),label = dist_name)
plt.legend()
plt.xticks(rotation = 45)

plt.tight_layout()
plt.show()
def combined_excel():
    with pd.ExcelWriter('./Test_Reports/missinglids.xlsx') as writer:
        #ml_ncs.to_excel(writer,sheet_name = 'NC - Missing Lids',index = False)
        ml_cmplnts.to_excel(writer,sheet_name = 'Cmplnts - Missing Lids',index = False)
combined_excel()


