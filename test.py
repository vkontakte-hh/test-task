import requests

def request():
    params = {"db": "vkontakte",
              "query": """
                        CREATE TABLE vkontakte.advertisements_performance_data(hour UInt32,
                                                               advertisementId UInt64,
                                                               locationId UInt64,
                                                               performanceRatio UInt8,
                                                               samplingRate UInt8) 
                        ENGINE = MergeTree() PARTITION BY hour
                        ORDER BY (advertisementId, locationId, hour)
                        SETTINGS index_granularity = 8192;
                        """}
    url = 'https://rc1b-2kg8g5lblno2pln0.mdb.yandexcloud.net:8443/'
    auth = {
        'X-ClickHouse-User': 'user-vk',
        'X-ClickHouse-Key': 'Qqwerty123',
    }

    res = requests.post(
        url,
        headers=auth,
        data=params,
        verify='/usr/local/share/ca-certificates/Yandex/YandexInternalRootCA.crt')
    res.raise_for_status()
    return res.text

print(request())
