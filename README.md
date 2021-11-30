

data_cleaner.py
    * currently connects to the excel workbook that holds the data connections to BPCS and Plantstar then cleans and prepares data for analysis and model building.  The clean data is then placed in the "clean_data" folder.  Reason for this is loading the raw data takes a long time and only needs to be done when processing a new model or forecasting.
model.py