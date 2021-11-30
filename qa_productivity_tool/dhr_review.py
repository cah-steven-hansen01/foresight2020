import pyperclip
import os
from nc_full import NC_Full
os.chdir(r'C:\Users\steven.hansen01\data_automation\venv')
from QMSPRODRP_report import QMSPRODRP_report



class DHR_Review():
    def __init__(self):
        print('Pulling Data...')
        self.lot_report = QMSPRODRP_report()
        print('-*'*5+' ignore the following OLE2 inconsistency warnings' +'*-'*5)
        self.lot_report.pull_lot_info()
        print('-*'*5+' ignore the above OLE2 inconsistency warnings' +'*-'*5)
        self.entered_lot = None
        self.lot_number = None
        self.lotqty = None
        self.eaches = None
        self.startdate= None
        self.enddate = None
        self.prod_dates = None
        self.product_family = None
        self.product_code = None
        os.system('cls')
        self.menu()

    def menu(self):
        _ = input('Press enter for main menu')
        os.system('cls')
        
        print('''

        Current lot = {}
        1. Enter new lot
        2. Enter lot information in manually
        3. Get manufacturing dates
        4. Display all lot data
        5. Get standard DHR Review verbiage
        6. Check Lot Number for NCs 
        7. Exit
                '''.format(self.entered_lot))
        menu_options = {1:self.enter_lot_num,
                        2:self.manual_info_entry,
                        3:self.manufact_dates,
                        4:self.print_lot_data,
                        5:self.dhr_review_verbiage,
                        6:self.check_lot_number_NC,
                        7:self.closedown}
        try:
            ans = int(input('Enter selection: '))
        except:
            print('Invalid input')
            self.menu()
        menu_options[ans]()
    def print_lot_data(self):
        os.system('cls')
        print('''
        lot number: {}
        start date: {}
        end date: {}
        product code: {}
        product family: {}
        lot qty (cs): {}
        lot qty (ea): {}
        shop order: {}
        '''.format(self.entered_lot,self.startdate,self.enddate,self.product_code,
        self.product_family,self.lotqty,self.eaches,self.shop_order))
        self.menu()
    def enter_lot_num(self):
        self.entered_lot = input("Lot Number: ")
        lot_info_dict = self.lot_report.lot_info(self.entered_lot)
        if str(lot_info_dict) == 'nan':
            print('Lot number not found in local database')
            self.menu()
        self.startdate = lot_info_dict['start date'].strftime('%m/%d/%Y')
        self.enddate = lot_info_dict['end date'].strftime('%m/%d/%Y')
        print(self.startdate + ' through '+self.enddate)
        correct_dates = input('Are these the correct dates? (y/n) ')
        if correct_dates == 'n':
            self.startdate = input('Correct start date: ')
            self.enddate = input('Correct end date: ')
        self._production_date_parser()
        self.product_code = str(lot_info_dict['product'])
        self.lot_number = lot_info_dict['lot number']
        self.lotqty = str(int(lot_info_dict['total in cases']))
        self.eaches = str(int(lot_info_dict['total in eaches']))
        self.product_family = str(lot_info_dict['product family'])
        self.shop_order = str(lot_info_dict['shop order'])
        self.menu()
    def _production_date_parser(self):
        if self.startdate != self.enddate:
            self.prod_dates = self.startdate + ' through '+self.enddate
        else:
            self.prod_dates = self.startdate
    def manufact_dates(self):
        if self.prod_dates == None:
            print('No dates entered')
        else:
            pyperclip.copy(self.prod_dates)
            print(self.prod_dates)
            print('check your clipboard!')
        self.menu()
        
    def dhr_review_verbiage(self):
        if None in [self.lot_number, self.eaches,self.prod_dates,self.product_family]:
            print('Not enough information provided')
            self.menu()
        areviewofthe = "A review of the device history record was completed for lot "
        thelotreleased =  "The lot released "
        #eaches = str(int(lotqty)*int(eachpercase))
        andwasproduced = " and was produced as part of the "
        finalstatement = "The DHR review concluded no abnormal process conditions were present during the manufacturing of this product that would lead to the reported condition.  The DHR review showed that all acceptance criteria inspections per established sampling levels were within acceptable limits during the production process. "
        DHRreview = str(areviewofthe+self.lot_number+"."+
                thelotreleased+str(self.lotqty)+" cases ("+self.eaches+" eaches) "+"on "+self.prod_dates+
                andwasproduced+self.product_family+" product family. "+ finalstatement)
        os.system('cls')
        print(DHRreview)
        pyperclip.copy(DHRreview)
        print('\ncheck clipboard!\n')


        self.menu()
    def manual_info_entry(self):

        self.lot_number = input('Lot number: ')
        self.entered_lot = self.lot_number
        self.lotqty = input('Lot qty (cs): ')
        self.eaches = input('Lot qty (ea): ')
        self.startdate= input('Start Date: ')
        self.enddate = input('End Date: ')
        self._production_date_parser()
        self.product_family = input('Product Family: ')
        self.product_code = input('Product Code: ')
        self.menu()
    def check_lot_number_NC(self):
        nc_full = NC_Full()
        nc_full.mostrecentreport()
        print(nc_full.check_for_lot_number(self.lot_number))
        self.menu()
    def closedown(self):
        os.system('cls')
        exit()
if __name__ == '__main__':
    DHR_Review()
