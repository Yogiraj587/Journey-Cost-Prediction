import requests
from bs4 import BeautifulSoup
import pandas as pd
import sqlite3
from datetime import date
import re


class ETL:

    def __init__(self, plaza_id, sql_file_path,sql_table_name):
        self.plaza_id = plaza_id
        self.sql_file_path = sql_file_path
        self.sql_table_name = sql_table_name
        self.url = f'https://tis.nhai.gov.in/TollInformation.aspx?TollPlazaID={plaza_id}'
        self.soup = ''
        self.df_info = pd.DataFrame

    def extract(self):
        r = requests.get(self.url)
        self.soup = BeautifulSoup(r.text, 'html.parser')
        if self.soup.find(class_='PA15'):
            return True
        return False

    def transform(self):
        plaza_name = self.soup.find(class_='PA15').find_all('p')[0].find('lable')
        distance = self.soup.find(class_="PA15").find_all('p')[0]
        dist1 = re.findall("Tollable Length : \d+",distance.text)
        dist2 = [int(re.findall("\d+", a)[0]) for a in dist1]
        dist3 = dist2[0]
        table_html = str(self.soup.find_all('table', class_='tollinfotbl')[0])
        df_info = pd.read_html(table_html)[0].dropna(axis=0, how='all')
        cols = df_info.columns.tolist()
        cols.insert(0, 'Date Scrapped')
        cols.insert(1, 'Plaza Name')
        cols.insert(2, 'TollPlazaID')
        cols.insert(3, 'Distance')
        df_info['Plaza Name'] = plaza_name.text
        df_info['Date Scrapped'] = date.today()
        df_info['TollPlazaID'] = self.plaza_id
        df_info['Distance'] = dist3
        self.df_info = df_info[cols]

    def load(self):
        with sqlite3.connect(self.sql_file_path) as conn:
            self.df_info.to_sql(self.sql_table_name, conn, if_exists='append', index=False)

    def run_etl(self):
        if self.extract():
            self.transform()
            self.load()
            print(f'Done with ETL of plaza_id {self.plaza_id}')
        else:
            print(f'Skipped plaza_id {self.plaza_id}')

