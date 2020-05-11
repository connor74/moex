import pandas as pd
import numpy as np
import requests

class Reprice:
#
# Выгрузка переоценки акций, переоценки облигаций и купонов
# 
    def __init__(self, date, output_path='N:\\AvdeevKB\\Общее\\', secid_path='N:\\AvdeevKB\\Общее\\'):
        self.secid = [] # Список isin бумаг портфеля
        self.date = date # Дата на которую нужно переоценить актив, формата YYYY-MM-DD
        self.output_path = output_path # Путь куда сохранять файлы
        self.secid_path = secid_path # Путь куда сохранять файлы
        self.secid_file = "Портфель ценных бумаг Банка.xlsx"
        self.boards = {
            'stocks': 'TQBR',
            'bonds': ['EQOB', 'TQCB', 'TQOB']
        }
               
       
    def read_secid(self, sheet_name):
        self.secid = pd.read_excel(self.secid_path+self.secid_file, sheet_name=sheet_name)
        
        try:
            self.secid = pd.read_excel(self.secid_path+self.secid_file, sheet_name=sheet_name)        
        except FileNotFoundError:
            print(f"Ошибка! Не найден файл: {self.sheet_name}_{self.date}.csv")  
    
    
    def get_stocks(self, dealer = True):
        
        df_ = pd.DataFrame()
        if dealer:
            self.sheet_name = "dealer_stocks"
        else:
            self.sheet_name = "client_stocks"
        
        self.read_secid(self.sheet_name)        

        hystory_link = f"http://iss.moex.com/iss/history/engines/stock/markets/shares/boards/{self.boards['stocks']}/securities.json"
        
        # Определение количества записей
        
        r = requests.get(hystory_link, params = {'date': self.date})
        TOTAL = r.json()['history.cursor']['data'][0][1]
        PAGESIZE = r.json()['history.cursor']['data'][0][2]
        k = TOTAL // PAGESIZE + 1 if TOTAL // PAGESIZE != TOTAL / PAGESIZE else TOTAL / PAGESIZE # количество итераций
        
        for i in range(0, k):
            jsn = pd.read_json(f"{hystory_link}?date={self.date}&start={i * 100 + 1}")
            temp = pd.DataFrame(jsn.loc['data','history'])
            df_ = pd.concat([df_, temp])
            
        df_.columns = jsn.loc['columns','history']      
        self.df = pd.merge(self.secid['SECID'], df_[['SECID', 'TRADEDATE', 'SHORTNAME', 'MARKETPRICE3']], how = 'left', on='SECID')


    def get_bonds(self, dealer = True):
        '''
        Получение с биржи рыночных котировок и купонов облигаций
        '''
        df_ = pd.DataFrame()
        if dealer:
            self.sheet_name = "dealer_bonds"
        else:
            self.sheet_name = "client_bonds"
        
        self.read_secid(self.sheet_name)    
        
        for item in np.array(self.secid):
            jsn = pd.read_json(f"http://iss.moex.com/iss/history/engines/stock/markets/bonds/securities/{item[0]}.json?"+
                               f"from={self.date}&till={self.date}")  
            if jsn.loc['data','history.cursor'][0][1] == 0:
                df_ = pd.concat([df_, pd.DataFrame({i:[np.nan] for i in jsn.loc['columns','history']})])
            else:
                temp = pd.DataFrame(jsn.loc['data','history'])
                temp.columns = jsn.loc['columns','history']
                temp = temp.loc[temp['BOARDID'].isin(self.boards['bonds'])]
                df_ = pd.concat([df_, temp])
                
        self.df = df_[['SECID', 'TRADEDATE', 'BOARDID', 'SHORTNAME', 'MARKETPRICE3', 'ACCINT']]
        
        
    def to_csv(self):
        '''
        Выгрузка в формате csv
        '''
        try:
            self.df.to_csv(self.output_path + self.sheet_name +'_' + self.date + '.csv', sep=';', encoding='utf-8-sig', index=False)
        except PermissionError:
            print(f"Ошибка! Возможно открыт файл: {self.sheet_name}_{self.date}.csv")  

    
    def view(self, pos=0):
        if pos > 0:
            return self.df.head(pos)
        elif pos < 0:
            return self.df.tail(abs(pos))
        else:
            return self.df
        '''
        Просмотр таблицы
        '''
        return self.df
        
