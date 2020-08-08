f """CREATE TABLE IF NOT EXISTS {self.db_name}.course_stat_USD_EUR_RUB 
                                                                (Day Date, 
                                                                USD Float64,
                                                                EUR Float64,
                                                                RUB Float64)
                                        ENGINE = MergeTree()
                                        PARTITION BY Day
                                        ORDER BY (Day)
                                        SETTINGS index_granularity = 8192 """
                                        
                                        
f """CREATE TABLE IF NOT EXISTS {self.db_name}.symbol_dict_USD_EUR_RUB 
                                                                (id UInt8,
                                                                 Symbol1 String,
                                                                 Symbol2 String,
                                                                 Symbol3 String)
                                             ENGINE = MergeTree()
                                             PARTITION BY id
                                             ORDER BY (id)
                                             SETTINGS index_granularity = 8192 """
        
f """CREATE TABLE IF NOT EXISTS {self.db_name}.mean_USD_EUR_RUB 
                                                                (Day Date,
                                                                 USD Float64,
                                                                 EUR Float64,
                                                                 RUB Float64)
                                             ENGINE = MergeTree()
                                             PARTITION BY Day
                                             ORDER BY (Day)
                                             SETTINGS index_granularity = 8192 """
       
