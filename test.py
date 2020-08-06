import requests

def request():
    body = """ INSERT INTO vkontakte.api_try VALUES (11, 11, 11, 11, 11) """
    url = 'https://rc1b-2kg8g5lblno2pln0.mdb.yandexcloud.net:8443/'
    auth = {
        'X-ClickHouse-User': 'user-vk',
        'X-ClickHouse-Key': 'Qqwerty123',
    }

    res = requests.post(
        url,
        body,
        headers=auth,
        verify='/usr/local/share/ca-certificates/Yandex/YandexInternalRootCA.crt')
    res.raise_for_status()
    return res.text

print(request())
