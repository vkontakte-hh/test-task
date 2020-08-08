import requests

class ch_task:
    def __init__(self, clickhouse_login, clickhouse_password, host, db_name, symbol_list):
        self.clickhouse_login = clickhouse_login
        self.clickhouse_password = clickhouse_password
        self.clickhouse_host = f'https://{host}.mdb.yandexcloud.net:8443/'
        self.db_name = db_name
        self.symbol = symbol_list
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
        symbol_str = '_'.join(self.symbol)
        query = f"""CREATE TABLE IF NOT EXISTS {self.db_name}.course_stat_{symbol_str} 
                                                                (Day Date, 
                                                                {self.symbol[0]} Float64,
                                                                {self.symbol[1]} Float64,
                                                                {self.symbol[2]} Float64)
                ENGINE = MergeTree()
                PARTITION BY Day
                ORDER BY (Day)
                SETTINGS index_granularity = 8192 """
        
        res = self.correction_query(query)
        return res
   
ch = ch_task("user-vk", "Qqwerty123", "rc1b-2kg8g5lblno2pln0", "vkontakte", ["USD", "EUR", "RUB"])
res = ch.check_or_create_tables()
print(res)
