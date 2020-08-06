import requests

def request():
    url = 'https://{host}:8443/?database={db}&query={query}'.format(
        host='rc1b-2kg8g5lblno2pln0.mdb.yandexcloud.net',
        db='vkontakte',
        query="CREATE TABLE supply(supply_id INT PRIMARY KEY AUTO_INCREMENT, title VARCHAR(50), author VARCHAR(30), price DECIMAL(8, 2), amount INT)"
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
