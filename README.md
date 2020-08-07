# ClickHouse и Python для хранения подневной статистикой кросс-курсов

## ClickHouse

### - Создаем кластер ClickHouse

### - Устанавливаем ClickHouse на ВМ

- Указываем репозиторий, с которого будет загружен ClickHouse
> ``` $ sudo apt-add-repository "deb http://repo.yandex.ru/clickhouse/deb/stable/ main/" ```

- Запускаем процесс установки ClickHouse
> ``` $ sudo apt-key adv --keyserver keyserver.ubuntu.com --recv E0C56BD4 ``` <br> 
> ``` $ sudo apt-get update ``` <br> 
> ``` $ sudo apt-get install clickhouse-client clickhouse-server ``` <br> 

- Устанавливаем сетртификат для дальнейшей работы с ClickHouse:
> ``` $ sudo mkdir -p ~/.clickhouse-client /usr/local/share/ca-certificates/Yandex ``` <br>
> ``` $ sudo wget "https://storage.yandexcloud.net/cloud-certs/CA.pem" -O /usr/local/share/ca-certificates/Yandex/YandexInternalRootCA.crt ``` <br>
> ``` $ sudo wget "https://storage.yandexcloud.net/mdb/clickhouse-client.conf.example" -O ~/.clickhouse-client/config.xml ``` <br>

- Запускаем ClickHouse
> ``` $	sudo service clickhouse-server start ```

- Команда для запуска консольного клиента
> ``` $ clickhouse-client ```

- Тестовый запрос к ClickHouse
> ``` $ select 1 ```
> Если все успешно, вернется 1.

Для смены пароля необходио отредактирвоать файл:
> ``` $ sudo nano /etc/clickhouse-server/users.d/default-password.xml ```

### - SQL синтаксис и команды ClickHouse

#### Создание таблиц:

- Выбираем <a href="https://clickhouse.tech/docs/ru/engines/table-engines/">движок</a> для будущей таблицы. Для решения данной задачи был выбран MergeTree.

Пример SQL-запроса для создания таблицы:
``` 
     CREATE TABLE db_name.table_name (hour UInt32,
                                      advertisementId UInt64,
                                      locationId UInt64,
                                      performanceRatio UInt8,
                                      samplingRate UInt8
                                     )
     ENGINE = MergeTree()
     PARTITION BY hour
     ORDER BY (advertisementId, locationId, hour)
     SETTINGS index_granularity = 8192 
```

#### Запись данных в таблицу:

Пример SQL-запроса для записи данных в таблицу: <br>
``` INSERT INTO db_name.table_name VALUES (12, 12, 12, 12, 12) ```

#### Выборка данных из таблицы:

Пример SQL-запроса на выборку данных: <br>
``` SELECT * FROM db_name.table_name WHERE advertisementId=12 ```

#### Выборка данных о партициях таблицы: <br>
Пример SQL-запроса на информации о партициях:
``` 
    SELECT partition,
           count() as number_of_parts,
           formatReadableSize(sum(bytes)) as sum_size
    FROM system.parts
    WHERE active 
          AND database = 'db_name'
          AND table = 'table_name'
    GROUP BY partition
    ORDER BY partition;
```
#### Обновление данных в таблице:

Пример SQL-запроса на обновление данных: <br>
``` ALTER TABLE db_name.table_name UPDATE advertisementId=77 WHERE locationId=12; ```

#### Удаление данных в таблице:

Пример SQL-запроса на удаление данных: <br>
``` ALTER TABLE db_name.table_name DELETE WHERE locationId=12; ```


## Yandex Cloud

1. Создаем проект в <a href="https://console.cloud.yandex.ru/" targt="_blank">Yandex.Cloud</a>
2. Создаем виртуальную машину

### Переходим к настройкам вирутального окружения

Для работы в Windows использовался PuTTY. Генерируется приватный и публичный ключ. После настройки подключения к ВМ устанавливаем и обновляем все необходимые пакеты.
> ``` $ sudo apt-get update ``` <br>
> ``` $ sudo apt-get upgrade ``` <br>

Проверяем наличие python, git, venv и всех необходимых пакетов. Если чего то не хватает, то устанавливаем:
> ``` $ sudo apt-get install python3 ``` <br>
> ``` $ sudo apt-get install git ``` <br>
> и т.д. <br>

Проверить наличие пакета можно командой:
> ``` $ python3 --version ```

Создаем вируальное окружение:
> ``` $ python3 -m venv vkontakte ```

Для активации окружения, команда:
> ``` $ python3 vkontakte/bin/activate ```

Для деактивации окружения, команда:
> ``` $ deactivate ```

После настройки виртуального окружения можно переносить код с репозитория в ВМ:
> ``` $ git clone https://github.com/vkontakte-hh/test-task.git ```

Устанавливаем зависимости:
> ``` $ pip install --upgrade -r requirements.txt ```

Обновить код из репозитория на ВМ с помощью команды:
> ``` $ git pull ```

## Выполнения SQL-запросов к ClickHouse с помощью Python

Выполнение запроса не связанного с изменением данных (запросы типа SELECT):
```
import requests

def request():
    url = 'https://{host}:8443/?database={db}&query={query}'.format(
        host='host.yandexcloud.net',
        db='db_name',
        query='SELECT now()')
    auth = {
        'X-ClickHouse-User': 'user_login',
        'X-ClickHouse-Key': 'user_password',
    }

    res = requests.get(
        url,
        headers=auth,
        verify='/usr/local/share/ca-certificates/Yandex/YandexInternalRootCA.crt')
    res.raise_for_status()
    return res.text

print(request())

```

Для запросов связанных с изменением данных (CREATE, UPDATE, DELETE и.д.) необходимо выполнить POST запрос на тот же эндпоинт:
```
import requests

def request():
    body = """ ALTER TABLE db_name.table_name DELETE WHERE advertisementId=11; """
    url = 'https://host.mdb.yandexcloud.net:8443/'
    auth = {
        'X-ClickHouse-User': 'user_login',
        'X-ClickHouse-Key': 'user_password',
    }

    res = requests.post(
        url,
        body,
        headers=auth,
        verify='/usr/local/share/ca-certificates/Yandex/YandexInternalRootCA.crt')
    res.raise_for_status()
    return res.text

print(request())
```

## Получение подневной статистикой кросс-курсов и рассчет скользяще средней цены с помощью Python

### Получение подневной статистикой кросс-курсов

Для получения данных использовался сайт <a href="https://fixer.io/">fixer.io</a>

- Собираем исторические данные по дням:
Выполняем GET запрос с праметрами:
> ``` http://data.fixer.io/api/YYYY-MM-DD?access_key=YOUR_ACCESS_KEY&base=JPY&symbols=USD,AUD,CAD,PLN,MXN ```

Валюты для отслежиания загружаются в код из отдельной таблицы ClickHouse.

В рамках бесплатного тарифа нет возможности выбирать базовую валюту.

- Если код ответа 200 и запрос выполнен успешно, то данные сохраняются в созданную ранее таблицу.
- Проверяется таблица в БД с партициями по дням и формируется словарь дат, статистика по которым по каким либо причинам отсутствует в базе.
- Получение статистики по тем дням, что были пропущены, если таковые имеются.

### Рассчет подневного значения скользящей средней цены за последние 28 дней

Вторая часть скрипта нацелена на вычисление подневного значения скользящей средней цены за последние 28 дней.

- Для этого собирается информация о доступных партициях в таблице.
- Формируется словарь с датами по которым отсутствует значение скользящей средней цены за последние 28 дней.
- Выбирается самая ранняя дата.
- Загружаются данные, период за 28 дней до самой ранней даты в словаре по самую позднюю дату.
- Рассчитываются значение скользящей средней цены за последние 28 дней.
- Полученный DataFrame фильтруется по всем датам из словаря.
- Полученная выборка записывается в соответствующую таблицу.

[Код описанного выше процесса в файле](test.py)
