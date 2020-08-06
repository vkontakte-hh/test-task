import requests

def request():
    body = """ ALTER TABLE vkontakte.api_try UPDATE samplingRate=77 WHERE advertisementId=11; """
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
