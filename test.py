import requests

def request():
    url = 'https://{host}:8443/?database={db}&query={query}'.format(
        host='rc1b-2kg8g5lblno2pln0.mdb.yandexcloud.net',
        db='vkontakte',
        query='SELECT * FROM vkontakte.advertisements_data WHERE hour IN (17, 16, 15)'
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
