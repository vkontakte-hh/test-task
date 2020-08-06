import requests

def request():
    url = 'https://{host}:8443/?database={db}&query={query}'.format(
        host='rc1b-2kg8g5lblno2pln0.mdb.yandexcloud.net',
        db='vkontakte',
        query="CREATE TABLE vkontakte.pare(id UInt8, Pare1 String, Pare2 String, Pare3 String) ENGINE = MergeTree() PARTITION BY id ORDER BY (id) SETTINGS index_granularity = 8192"
    )
    auth = {
        'X-ClickHouse-User': 'user-vk',
        'X-ClickHouse-Key': 'Qqwerty123',
    }

    res = requests.get(
        url,
        headers=auth,
        verify='/usr/local/share/ca-certificates/Yandex/YandexInternalRootCA.crt')
    res.raise_for_status()
    return res.text

print(request())
