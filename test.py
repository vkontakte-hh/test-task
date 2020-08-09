import requests
from datetime import datetime, timedelta
import pandas as pd

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
        
        create_mean_table_query = f"""CREATE TABLE IF NOT EXISTS {self.db_name}.mean_USD_EUR_RUB 
                                                                (Day Date,
                                                                 USD Float64,
                                                                 EUR Float64,
                                                                 RUB Float64)
                                             ENGINE = MergeTree()
                                             PARTITION BY Day
                                             ORDER BY (Day)
                                             SETTINGS index_granularity = 8192 """
        
        self.correction_query(create_mean_table_query) # Создаем таблицу для хранения скользящей средней
        
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
        
    def get_currency_hystory(self, date_range=None):
        symbols = self.db_connect.get_table_data(f"SELECT Symbol1, Symbol2, Symbol3 FROM {self.db_name}.symbol_dict_USD_EUR_RUB")
        if date_range is None:
            date_range = [datetime.strftime(datetime.strptime(self.date_from, "%Y-%m-%d") + timedelta(days=x), "%Y-%m-%d") for x in range(0, (datetime.strptime(self.date_to, "%Y-%m-%d") - datetime.strptime(self.date_from, "%Y-%m-%d")).days + 1)]
        for date in date_range:
            response = requests.get(self.URL + date, params = {"symbols": ','.join(symbols[0]), 
                                                               "access_key": self.access_key})
            
            response = self.chech_currency_hystory_success(response)
            self.db_connect.insert_data(f"INSERT INTO {self.db_name}.course_stat_USD_EUR_RUB VALUES ('{date}', {response['USD']}, {response['EUR']}, {response['RUB']})")
            
        return []
    
    def get_missing_data(self):
        skip_date_list = self.db_connect.get_partition('course_stat_USD_EUR_RUB')
        st_date = "2018-01-01"
        en_date = datetime.strftime(datetime.today() - timedelta(days=1), "%Y-%m-%d")
        all_date_range = [datetime.strftime(datetime.strptime(st_date, "%Y-%m-%d") + timedelta(days=x), "%Y-%m-%d") for x in range(0, (datetime.strptime(en_date, "%Y-%m-%d") - datetime.strptime(st_date, "%Y-%m-%d")).days + 1)]
        diff_list = list(set(all_date_range).difference(set(skip_date_list)))
        
        self.get_currency_hystory(diff_list)
        
        return []
    
    def count_roll_mean(self):
        result_df = pd.DataFrame() 
        # Создаем пустой DataFrame
        data = self.db_connect.get_table_data(f"SELECT * FROM {self.db_name}.course_stat_USD_EUR_RUB") 
        # Загружаем статистику по курсам валют за весь период
        df = pd.DataFrame(data, columns=["Day", "USD", "EUR", "RUB"]) 
        # СОздаем DataFrame из полученной статистики
        result_df['Day'] = df['Day'] 
        # Создаем столбец с датами в пустом DataFrame
        column_list = list(df.columns) 
        # Получаем все столбцы из DataFrame со статистикой
        column_list.remove("Day") 
        # Удаляем название столбца с датой
        for element in column_list: 
            # Для каждого оставшегося столбца (со статистикой по каждой валюте) считаем среднее скользящее за 28 дней и добавляем в пустой DataFrame полученный столбец значений
            roll = df[element].rolling(window=28)
            result_df[element] = roll.mean().fillna(0)
        data_in_mean_table = self.db_connect.get_table_data(f"SELECT * FROM {self.db_name}.mean_USD_EUR_RUB") 
        # Получаем имеющиеся данные из DataFrame со статистикой по средней скользящей
        data_in_mean_table_df = pd.DataFrame(data_in_mean_table, columns=["Day", "USD", "EUR", "RUB"])
        
        difference_to_insert = result_df[~(result_df['Day'].isin(data_in_mean_table_df['Day'])] 
        # Отсекаем данные из DataFrame со всей статистикой те строки, которые уже есть в ClickHouse
        difference_to_insert_in_list = list(difference_to_insert.T.to_dict().values()) 
        # Преобразуем DataFrame в лист из словарей со значениями
        for insert_element in difference_to_insert_in_list: 
            # В цикле записываем в ClickHouse строки, которых там нет (по прупущенным дням или по новым)
            self.db_connect.insert_data(f"INSERT INTO {self.db_name}.mean_USD_EUR_RUB VALUES ({insert_element['Day']}, {insert_element['USD']}, {insert_element['EUR']}, {insert_element['RUB']})")
        return []
        
    
access_key = "c99033bf278986db036c4344d9d40f4a"
date_from = datetime.strftime(datetime.today() - timedelta(days=1), "%Y-%m-%d") 
# Берем вчерашний день как день старта
date_to = datetime.strftime(datetime.today() - timedelta(days=1), "%Y-%m-%d") 
# Берем вчерашний день как день конечную дату
clickhouse = ch_task("user-vk", "Qqwerty123", "rc1b-2kg8g5lblno2pln0", "vkontakte") 
# Создаем подключение к ClickHouse и экземпляр класса
clickhouse.check_or_create_tables() 
# Проверяем созданы ли таблицы, если нет, то они создаются и в таблицу-словарь загружаются прописанные в коде валютные пары для отслеживания
vk = vk_task(date_from, date_to, access_key, "user-vk", "Qqwerty123", "rc1b-2kg8g5lblno2pln0", "vkontakte") 
# Создаем экземпляр класса для работы с fixer
vk.get_currency_hystory() 
# Получаем статистику за вчерашний день и записываем в ClickHouse
vk.get_missing_data() 
# Получаем статистику за пропущенные дня (период с "2018-01-01" по вчерашний день) и записываем в ClickHouse
vk.count_roll_mean() 
# Рассчитываем среднюю скользящую за весь период по дням для каждой валюты и записываем в ClickHouse те строки, по которым данные отсутвуют
