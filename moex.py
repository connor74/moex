import pandas as pd
import numpy as np
import requests

class Reprice:

    def __init__(self, date):
        self.secid = [] 
        self.date = date # YYYY-MM-DD
        self.df = pd.DataFrame()
        self.boards = {
            'shares': 'TQBR',
            'bonds': ['EQOB', 'TQCB', 'TQOB']
        }
        self.sec_type = ""
        

    def secid_from_csv(self, file, header=None):
        '''
        Getting SECID from csv file, without headers.
        Data in first column
        '''
        try:
            df = pd.read_csv(file, header=header)
        except FileNotFoundError:
            print(f"Error! File: {file} is not found:")   
        self.secid = list(df.iloc[:, 0])


    def secid_from_excel(self, file, sheet=0, header=None):
        '''
        Getting SECID from xls/xlsx file, without headers.
        Data in first column
        '''
        try:
            df = pd.read_excel(file, sheet_name=sheet, header=header)
        except FileNotFoundError:
            print(f"Error! File: {file} is not found:")  
        self.secid = list(df.iloc[:, 0])

    
    def secid_from_list(self, secid_list):
        self.secid = secid_list


    def __get_securities(self, sec_type, columns, boards):
        '''
        Получение с биржи рыночных котировок акций
        '''
        if self.secid == []: raise ValueError("SECID is empty!")

        self.sec_type = sec_type

        row = []
        for item in self.secid:
            link = f"http://iss.moex.com/iss/history/engines/stock/markets/{sec_type}/securities/{item}.json"
            jsn = requests.get(link, params = {"from": self.date, "till": self.date}).json()
            if len(jsn['history']['data']) == 0:
                row_ = [[np.nan] * len(jsn['history']['columns'])]
            elif len(jsn['history']['data']) > 1:
                row_ = [item for item in jsn['history']['data'] if item[0] in boards]
            else:
                row_ = jsn['history']['data']
            row = [*row, *row_]
        return pd.DataFrame(row, columns = jsn['history']['columns'])[columns]

    
    def get_stocks(self):
        columns = ['SECID', 'TRADEDATE', 'BOARDID', 'SHORTNAME', 'MARKETPRICE3']
        self.df = self.__get_securities('shares', columns, self.boards['stocks'])


    def get_bonds(self):
        columns = ['SECID', 'TRADEDATE', 'BOARDID', 'SHORTNAME', 'MARKETPRICE3', 'ACCINT']
        self.df = self.__get_securities('bonds', columns, self.boards['bonds'])


    def to_csv(self, output_path="", file_name="data"):
        try:
            self.df.to_csv(f'{output_path}{file_name}_{self.sec_type}_{self.date}.csv', sep=';', encoding='utf-8-sig', index=False)
        except PermissionError:
            print(f"Error! Maybe file is opened: _{self.date}.csv")  

    
    def view(self, pos=0):
        if pos > 0:
            return self.df.head(pos)
        elif pos < 0:
            return self.df.tail(abs(pos))
        else:
            return self.df
        return self.df

class GCurve:
    def __init__(self, date):
        self.date = date
        self.secid_list = []

    def get_isin_list(self):
        pass

    def get_secid_list(self):
        pass


