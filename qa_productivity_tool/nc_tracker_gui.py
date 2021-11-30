import tkinter as tk
from tkinter import ttk,filedialog
from tkinter import scrolledtext
from tkinter import Menu
from tkinter import messagebox as msg
from tkinter import Spinbox
from time import  sleep 
import os

from matplotlib.backends.backend_tkagg import (
    FigureCanvasTkAgg, NavigationToolbar2Tk)
from matplotlib.backend_bases import key_press_handler
from matplotlib.figure import Figure

import datetime as dt
import pandas as pd

from ncr_mrb import NCR_MRB
from nc_full import NC_Full
from nc_task_phase import NC_Task_Phase



class NC_Tracker_GUI:
    def __init__(self):
        self.win = tk.Tk() 
        #self.win.geometry("1300x900") 
        self.win.title("NC Tracker")
        cwd  = os.getcwd().split('\\')
        self.og_folder_path = cwd[0]+'\\'+cwd[1]+'\\'+cwd[2]+'\\'+"Downloads"
        self.search_reports()
        self.create_widgets()
        self.run_mrb_report()
        self.populate_open_nc_tree()
        self.populate_open_metrics_tree()
    def search_reports(self):
        print(f'search reports called from {self.og_folder_path}')
        self.nc_mrb = NCR_MRB( )
        meta_features = ['r_filename','path','date']
        self.nc_mrb_meta_data = {'name':'NC MRB'}
        for f,v in zip(meta_features,self.nc_mrb.mostrecentreport(og_data_path = self.og_folder_path)):
            self.nc_mrb_meta_data[f] = v
        mrb_report_date = str(pd.to_datetime(self.nc_mrb_meta_data['date'],format='%m/%d/%Y'))[:10]
        today_date = str(pd.to_datetime(dt.datetime.now(),format='%m/%d/%Y'))[:10]
        self.nc_full = NC_Full()
        self.nc_full_meta_data = {'name':'NC Full'}
        for f, v in zip(meta_features, self.nc_full.mostrecentreport(og_data_path = self.og_folder_path)):
            self.nc_full_meta_data[f] = v
        full_report_date = str(pd.to_datetime(self.nc_mrb_meta_data['date'],format='%m/%d/%Y'))[:10]
        self.nc_task_phase = NC_Task_Phase()
        self.nc_task_phase_meta_data= {'name': 'NC Average Time to Close Task'}
        for f, v in zip(meta_features, self.nc_task_phase.mostrecentreport(og_data_path = self.og_folder_path)):
            self.nc_task_phase_meta_data[f] = v
        self.all_meta_data = [self.nc_mrb_meta_data,self.nc_full_meta_data,self.nc_task_phase_meta_data]
    
    def create_widgets(self):
        tabControl = ttk.Notebook(self.win)          # Create Tab Control
        self.tab1 = ttk.Frame(tabControl)
        tabControl.add(self.tab1, text='NCs - Current State')      # Add the tab
        self.tab2 = ttk.Frame(tabControl)
        tabControl.add(self.tab2, text = "Daily Tracker",state = 'normal')
        self.tab3 = ttk.Frame(tabControl)            # Add a second tab
        tabControl.add(self.tab3, text='Monthly Tracker',state =  'normal')      # Make second tab visible
        tab4 = ttk.Frame(tabControl)
        tabControl.add(tab4, text = 'Ad Hoc',state = 'disabled')
        tabControl.pack(expand=1, fill="both")
        # TOP FRAMES
        top_frame_tab1 = ttk.LabelFrame(self.tab1,labelanchor = 'n')
        top_frame_tab1.grid(column = 0,row = 0,columnspan = 4)
        top_frame_tab2 = ttk.LabelFrame(self.tab2,labelanchor = 'n')
        top_frame_tab2.grid(column = 0,row=0,columnspan = 4)
        # OPTIONS
        tab1_options_frame = ttk.LabelFrame(top_frame_tab1,text = 'Options',labelanchor = 'n')
        tab1_options_frame.grid(column = 0,row =0,pady = 7,padx = 89,sticky = 'n')
        # REPORT INFORMATION
        sub_top_frame = ttk.LabelFrame(top_frame_tab1,text = 'Report Information',labelanchor='n')
        sub_top_frame.grid(column = 1,row =0,pady = 7,padx = 89)
        # meta table
        tab1_meta_table = ttk.LabelFrame(sub_top_frame,text = 'Original Data',labelanchor = 'n')
        tab1_meta_table.grid(column = 1,row= 0,sticky = 'n',pady = 7,padx = 7,columnspan = 2 )
        # Current file path button
        self.folder_select_button = ttk.Button(tab1_meta_table, text = 'Current File Path: '+str(self.og_folder_path), command = self.click_folder_select)
        self.folder_select_button.grid(column = 0,row = 1,pady = 7,padx = 7,columnspan = 2)
        # metrics table
        tab1_metric_fram = ttk.LabelFrame(sub_top_frame,text = 'Metrics',labelanchor = 'n')
        tab1_metric_fram.grid(column=1,row = 2,sticky = 's',pady = 7,padx = 7)
        
        # visuals buttons
        tab1_visual_frame = ttk.LabelFrame(sub_top_frame,text = 'Visuals',labelanchor = 'n')
        tab1_visual_frame.grid(column = 2,row=2,pady=7,padx = 7)
        # OPEN NCS
        tab1_open_ncs = ttk.LabelFrame(self.tab1,text='Open NCs',labelanchor = 'n')
        tab1_open_ncs.grid(column = 0,row = 1,sticky = 'w',columnspan = 4)
        # METADATA TABLE
        self.meta_tree = ttk.Treeview(tab1_meta_table)
        self.meta_tree['height'] = 1
        self.meta_tree['columns'] = ('Report Name','Created Date','Filename')
        self.meta_tree.column('#0',width = 40,minwidth = 25)
        self.meta_tree.column('Report Name',anchor = 'w',width = 160,minwidth = 25)
        self.meta_tree.column('Created Date',anchor = 'w',width = 80,minwidth = 25)
        self.meta_tree.column('Filename',anchor = 'w',width = 300,minwidth = 25)        
        self.meta_tree.heading("#0",text = 'Label')
        self.meta_tree.heading('Report Name', text = 'Report Name',anchor = 'w')
        self.meta_tree.heading("Created Date", text = 'Created Date',anchor = 'w')
        self.meta_tree.heading('Filename', text = 'Filename',anchor = 'w')
        self.meta_tree.grid(column = 1,row=0,sticky = 'w',pady = 7,padx = 7,columnspan = 3)
        self.populate_meta_tree()
        # OPEN NCS TABLE
        self.open_tree = ttk.Treeview(tab1_open_ncs)
        self.open_tree['height'] = 25
        self.open_tree['columns'] = ('NC Number','Created Date','Description','Initial Failure Mode',
        'Task Name','Task Owner','Age')
        self.open_tree.column("#0",width = 40,minwidth =25)
        self.open_tree.column("NC Number",anchor = 'w', width = 120,minwidth = 25)
        self.open_tree.column("Created Date",anchor = 'w', width = 80,minwidth =25)
        self.open_tree.column("Description",anchor = 'w',width = 240,minwidth = 25)
        self.open_tree.column("Initial Failure Mode",anchor = 'w',width = 240,minwidth = 25)
        self.open_tree.column("Task Name",anchor = 'w',width = 240,minwidth = 25)
        self.open_tree.column("Task Owner",anchor = 'w',width = 240,minwidth = 25)
        self.open_tree.column("Age",anchor = 'center',width = 80,minwidth = 25)
        self.open_tree.heading("#0",text = 'Label')
        self.open_tree.heading("NC Number", text = 'NC Number',anchor = 'w')
        self.open_tree.heading("Created Date",text = 'Created Date',anchor = 'w')
        self.open_tree.heading("Description",text = "Description",anchor = 'w')
        self.open_tree.heading('Initial Failure Mode',text = 'Initial Failure Mode',anchor = 'w')
        self.open_tree.heading("Task Name",text = "Task Name",anchor = 'w')
        self.open_tree.heading("Task Owner",text = "Task Owner",anchor = 'w')
        self.open_tree.heading('Age',text = "Age",anchor = 'center')
        self.open_tree.grid(column = 0,row=2,sticky = 'sw',pady = 7,padx = 7,columnspan = 8)
        # METRICS TABLE
        # METRICS FRAME
        self.open_metric_tree = ttk.Treeview(tab1_metric_fram)
        self.open_metric_tree['height'] = 1
        self.open_metric_tree['columns'] = ('Percent open under 60 days','Count of open NCs')
        self.open_metric_tree.column("#0",width = 0)
        self.open_metric_tree.column('Percent open under 60 days',anchor ='center',width = 160)
        self.open_metric_tree.column('Count of open NCs',anchor = 'center',width = 160)
        self.open_metric_tree.heading('Percent open under 60 days',text = 'Percent open under 60 days',anchor = 'w')
        self.open_metric_tree.heading('Count of open NCs',text = 'Count of open NCs',anchor = 'w')
        #self.open_metric_tree.heading("#0",text = 'Label')
        self.open_metric_tree.grid(column=0,row = 0,sticky = 'nw',pady = 7,padx = 7, columnspan = 4)
        # OPTIONS FRAME
        # REFRESH BUTTON
        self.refresh_button = ttk.Button(tab1_options_frame,text = 'Refresh Original Data',command = self.refresh_search)
        self.refresh_button.grid(column = 0,row = 0,pady = 7,padx = 7)
        # open nc task tracker button
        self.teir_button = ttk.Button(tab1_options_frame,text = 'Open "NC Task Tracker.xlsx"',command = self.click_teirII )
        self.teir_button.grid(column =0,row = 1,pady = 7,padx = 7)
        # COPY OPEN NCs Button
        self.copy_open_button = ttk.Button(tab1_options_frame,text = "Send Open NCs to Clipboard", command = self.clipboard_mrb )
        self.copy_open_button.grid(column = 0,row = 3,pady = 7 ,padx= 7)
        # VISUAL FRAME
        self.open_nc_age = ttk.Button(tab1_visual_frame,text = 'Open NCs by age',command = self.click_open_nc_age)
        self.open_nc_age.grid(column=0,row=0,pady = 7,padx = 7)
        self.nc_age_dist= ttk.Button(tab1_visual_frame,text = 'Age Distribution',command = self.click_nc_age_dist)
        self.nc_age_dist.grid(column=0,row=1,pady = 7,padx = 7,sticky = 'nesw')
        # PROGRESS BAR TAB1
        self.prog_bar_tab1 = ttk.Progressbar(top_frame_tab1,orient = 'horizontal',length = 286,mode = 'determinate')
    def run_prog_bar_tab1(self,static = False):
        self.prog_bar_tab1.grid(column = 0,row = 2,padx = 7,pady = 7,sticky = 'n')
        self.prog_bar_tab1['maximum'] = 100
        for i in range(101):
            sleep(0.025)
            self.prog_bar_tab1['value'] = i
            self.prog_bar_tab1.update()
        #self.prog_bar_tab1['value'] = 0
        self.prog_bar_tab1.grid_forget()
    def start_prog_bar_tab1(self):
        self.prog_bar_tab1.grid(column = 0,row = 2,padx = 7,pady = 7,sticky = 'n')
        self.prog_bar_tab1['value'] = 100
        self.prog_bar_tab1.update()
    def stop_prog_bar_tab1(self):
        self.prog_bar_tab1.grid_forget()
    def populate_meta_tree(self):
        for i,report in enumerate(self.all_meta_data):
            name = report['name']
            created_date = report['date']
            r_filename = report['r_filename']
            #path = report['path']
            self.meta_tree.insert(parent = '',values =(name,created_date,r_filename),index = 'end',text = i+1 )
    def populate_open_nc_tree(self):
        for i,nc in enumerate(self.open_ncs_df['NC Number']):
            created_date = self.open_ncs_df.loc[i,'Created Date']
            descrip = self.open_ncs_df.loc[i,'Description']
            ifm = self.open_ncs_df.loc[i,'Initial Failure Mode']
            task_name = self.open_ncs_df.loc[i,'Task Name']
            task_owner = self.open_ncs_df.loc[i,'Task Owner']
            age = self.open_ncs_df.loc[i,'Age of NC']
            self.open_tree.insert(parent = '',index = 'end',iid = i,text = i+1,values = (nc,created_date,descrip,ifm,task_name,task_owner,age) )   
    def populate_open_metrics_tree(self):
        self.open_metric_tree.insert(parent = '',index = 'end',iid=0,text = 1,values = (self.open_under_60,self.num_of_open))
    def refresh_search(self):
        print('refresh!')
        self.meta_tree.delete(*self.meta_tree.get_children())
        self.open_tree.delete(*self.open_tree.get_children())
        self.open_metric_tree.delete(*self.open_metric_tree.get_children())
        self.run_prog_bar_tab1()
        self.search_reports()
        self.populate_meta_tree()
        self.run_mrb_report()
        self.populate_open_nc_tree()
        self.populate_open_metrics_tree()
    def run_mrb_report(self):
        self.run_prog_bar_tab1()
        self.open_ncs_df, _ = self.nc_mrb.run_report()
        self.num_of_open,self.open_under_60 = self.nc_mrb.metrics()
        self.open_under_60 = self.open_under_60*100
        self.prog_bar_tab1['value'] = 0
    def click_folder_select(self):
        self.og_folder_path = filedialog.askdirectory(initialdir = self.og_folder_path ,title = "Select Folder")
        self.folder_select_button['text'] =  'Current File Path: '+str(self.og_folder_path)
        self.refresh_search()
        print(self.og_folder_path)
    def click_teirII(self):
        self.run_prog_bar_tab1()
        self.nc_mrb.visual_tierII_tracker(openfile = 'y',save_to_path='./')
    def click_open_nc_age(self):
        self.start_prog_bar_tab1()
        self.nc_mrb.visual_NC_age_bar(show = True)
        self.stop_prog_bar_tab1()
    def click_nc_age_dist(self):
        self.start_prog_bar_tab1()
        self.nc_mrb.visual_age_distribution(show = True)
        self.stop_prog_bar_tab1()
    def clipboard_mrb(self):
        self.start_prog_bar_tab1()
        self.open_ncs_df.to_clipboard(index=False)
        self.stop_prog_bar_tab1()
if __name__ == '__main__':
    nc_tracker_gui = NC_Tracker_GUI()
    nc_tracker_gui.win.mainloop()