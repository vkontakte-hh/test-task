import requests

class ch_task:
    def __init__(self, clickhouse_login, clickhouse_password, host, db_name, symbol_list):
        self.clickhouse_login = clickhouse_login
        self.clickhouse_password = clickhouse_password
        self.clickhouse_host = f'https://{host}.mdb.yandexcloud.net:8443/'
        self.db_name = db_name
        self.symbol = symbol_list
        self.symbol.sort()
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
        return response
    
    def check_or_create_tables(self):
        symbol_str = '_'.join(self.symbol)
        create_course_table_query = f"""CREATE TABLE IF NOT EXISTS {self.db_name}.course_stat_{symbol_str} 
                                                                (Day Date, 
                                                                {self.symbol[0]} Float64,
                                                                {self.symbol[1]} Float64,
                                                                {self.symbol[2]} Float64)
                                        ENGINE = MergeTree()
                                        PARTITION BY Day
                                        ORDER BY (Day)
                                        SETTINGS index_granularity = 8192 """
        
        self.correction_query(create_course_table_query) # Создаем таблицу для хранения подневной статистики
        
        create_symbol_dict_table_query = f"""CREATE TABLE IF NOT EXISTS {self.db_name}.symbol_dict_{symbol_str} 
                                                                (id UInt8,
                                                                 Symbol1 String,
                                                                 Symbol2 String,
                                                                 Symbol3 String)
                                             ENGINE = MergeTree()
                                             PARTITION BY id
                                             ORDER BY (id)
                                             SETTINGS index_granularity = 8192 """
        
        self.correction_query(create_symbol_dict_table_query) # Создаем таблицу для хранения отслеживаемых курсов валют
        
        delete_data_in_table_dict = f""" ALTER TABLE {self.db_name}.symbol_dict_{symbol_str} 
                                         DELETE WHERE Symbol1 != '';"""
        
        self.correction_query(delete_data_in_table_dict) # Очищаем таблицу словаря
        
        insert_data_in_table_dict = f""" INSERT INTO {self.db_name}.symbol_dict_{symbol_str} 
                                         VALUES ({self.symbol[0]}, 
                                                 {self.symbol[1]}, 
                                                 {self.symbol[2]})"""
        
        self.correction_query(insert_data_in_table_dict) # Записываем отслеживаемые валюты в словарь
        
        return []
    
    def get_skip_days(self):
        symbol_str = '_'.join(self.symbol)
        select_skip_days_query = f""" 
                                     SELECT partition
                                     FROM system.parts
                                     WHERE active
                                           AND database = {self.db_name}
                                           AND table = course_stat_{symbol_str}
                                           ORDER BY partition;
        """
        skip_days = self.select_query(select_skip_days_query)
        return skip_days

    
ch = ch_task("user-vk", "Qqwerty123", "rc1b-2kg8g5lblno2pln0", "vkontakte", ["USD", "EUR", "RUB"])
ch.check_or_create_tables()
response = ch.get_skip_days()
print(type(response), response)
