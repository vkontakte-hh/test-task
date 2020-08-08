import requests
from datetime import datetime, timedelta

class ch_task:
    def __init__(self, clickhouse_login, clickhouse_password, host, db_name):
        self.clickhouse_login = clickhouse_login
        self.clickhouse_password = clickhouse_password
        self.clickhouse_host = f'https://{host}.mdb.yandexcloud.net:8443/'
        self.db_name = db_name
        self.auth = {'X-ClickHouse-User': clickhouse_login,
                     'X-ClickHouse-Key': clickhouse_password}
        
    def correction_query(self, query):
        response = requests.post(self.clickhouse_host,
                                 query,
                                 headers=self.auth,
                                 verify='/usr/local/share/ca-certificates/Yandex/YandexInternalRootCA.crt')
        response.raise_for_status()
        return response.text
    
    def select_query(self, query):
        params = {"database": self.db_name, "query": query}
        response = requests.get(self.clickhouse_host,
                                headers=self.auth,
                                params=params,
                                verify='/usr/local/share/ca-certificates/Yandex/YandexInternalRootCA.crt')
        response.raise_for_status()
        return response.text
    
    def check_or_create_tables(self):
        create_course_table_query = f"""CREATE TABLE IF NOT EXISTS {self.db_name}.course_stat_USD_EUR_RUB 
                                                                (Day Date, 
                                                                USD Float64,
                                                                EUR Float64,
                                                                RUB Float64)
                                        ENGINE = MergeTree()
                                        PARTITION BY Day
                                        ORDER BY (Day)
                                        SETTINGS index_granularity = 8192 """
        
        self.correction_query(create_course_table_query) # Создаем таблицу для хранения подневной статистики
        
        create_symbol_dict_table_query = f"""CREATE TABLE IF NOT EXISTS {self.db_name}.symbol_dict_USD_EUR_RUB 
                                                                (id UInt8,
                                                                 Symbol1 String,
                                                                 Symbol2 String,
                                                                 Symbol3 String)
                                             ENGINE = MergeTree()
                                             PARTITION BY id
                                             ORDER BY (id)
                                             SETTINGS index_granularity = 8192 """
        
        self.correction_query(create_symbol_dict_table_query) # Создаем таблицу для хранения отслеживаемых курсов валют
        
        delete_data_in_table_dict = f""" ALTER TABLE {self.db_name}.symbol_dict_USD_EUR_RUB 
                                         DELETE WHERE Symbol1 != '';"""
        
        self.correction_query(delete_data_in_table_dict) # Очищаем таблицу словаря
        
        insert_data_in_table_dict = f""" INSERT INTO {self.db_name}.symbol_dict_USD_EUR_RUB 
                                         VALUES (1,
                                                 'USD', 
                                                 'EUR', 
                                                 'RUB')"""
        
        self.correction_query(insert_data_in_table_dict) # Записываем отслеживаемые валюты в словарь
        
        return []
    
    def get_partition(self, tb_name):
        select_part_query = f""" SELECT partition FROM system.parts 
                                      WHERE active 
                                      AND database = '{self.db_name}' 
                                      AND table = '{tb_name}'
                                      ORDER BY partition;
        """
        part = self.select_query(select_part_query)
        part_list = part.split('\n')
        part_list.remove('')
        return list(set(part_list))
    
    def get_table_data(self, query):
        response = self.select_query(query)
        response_list = list(response.split("\n"))
        response_list.remove('')
        response_list = [x.split('\t') for x in response_list]
        return response_list
    
    def insert_data(self, query):
        self.correction_query(query)
        return []


class vk_task:
    def __init__(self, date_from, date_to, access_key, clickhouse_login, clickhouse_password, host, db_name):
        self.URL = "http://data.fixer.io/api/"
        self.date_from = date_from
        self.date_to = date_to
        self.db_name = db_name
        self.access_key = access_key
        self.db_connect = ch_task(clickhouse_login, clickhouse_password, host, db_name)
    
    def chech_currency_hystory_success(self, response):
        if response.status_code == 200:
            response_data = response.json()
            status = response_data['success']
            if status:
                return response_data['rates']
            else:
                pass
        else:
            pass
        
    def get_currency_hystory(self, date_from=None, date_to=None):
        if (date_from is not None) & (date_to is not None):
            self.date_from = date_from
            self.date_to = date_to
        date = self.date_from
        while (datetime.strftime(datetime.strptime(self.date_to, "%Y-%m-%d") + timedelta(days=1), "%Y-%m-%d") != date) & (date != datetime.strftime(datetime.today(), "%Y-%m-%d")):
            response = requests.get(self.URL + date, params = {"symbols": 'USD,EUR,RUB', 
                                                               "access_key": self.access_key})
            
            response = self.chech_currency_hystory_success(response)
            self.db_connect.insert_data(f"INSERT INTO {self.db_name}.course_stat_USD_EUR_RUB VALUES ('{date}', {response['USD']}, {response['EUR']}, {response['RUB']})")
            date = datetime.strftime(datetime.strptime(date, "%Y-%m-%d") + timedelta(days=1), "%Y-%m-%d")
            
        return []
    
    def get_missing_data(self):
        skip_date_list = self.db_connect.get_partition('course_stat_USD_EUR_RUB')
        st_date = "2018-01-01"
        en_date = datetime.strftime(datetime.today() - timedelta(days=1), "%Y-%m-%d")
        date_range = [datetime.strftime(datetime.strptime(st_date, "%Y-%m-%d") + timedelta(days=x), "%Y-%m-%d") for x in range(0, (datetime.strptime(en_date, "%Y-%m-%d") - datetime.strptime(st_date, "%Y-%m-%d")).days + 1)]
        diff_list = list(set(date_range).difference(set(skip_date_list)))
        return diff_list
    
access_key = "c99033bf278986db036c4344d9d40f4a"
date_from = "2020-07-01"
date_to = "2020-07-05"
vk = vk_task(date_from, date_to, access_key, "user-vk", "Qqwerty123", "rc1b-2kg8g5lblno2pln0", "vkontakte")
data = vk.get_missing_data()
