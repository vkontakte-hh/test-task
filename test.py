import requests

def request():
    url = 'https://{host}:8443/?database={db}&query={query}'.format(
        host='rc1b-2kg8g5lblno2pln0.mdb.yandexcloud.net',
        db='vkontakte',
        query='CREATE TABLE t_pare_table(pare_id UInt8, Pare_one String, Pare_two String, Pare_three String) ENGINE = MergeTree() PARTITION BY pare_id ORDER BY (pare_id)'
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
