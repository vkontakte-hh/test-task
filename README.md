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

Пример SQL-запроса для записи данных в таблицу:
``` INSERT INTO db_name.table_name VALUES (12, 12, 12, 12, 12) ```

#### Выборка данных из таблицы:

Пример SQL-запроса на выборку данных:
``` SELECT * FROM db_name.table_name WHERE advertisementId=12 ```

#### Выборка данных о партициях таблицы:
Пример SQL-запроса на информации о партициях:
``` SELECT partition,
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

Пример SQL-запроса на обновление данных:
``` ALTER TABLE db_name.table_name UPDATE advertisementId=77 WHERE locationId=12; ```

#### Удаление данных в таблице:

Пример SQL-запроса на удаление данных:
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

## Выполнения SQL-запросов к ClickHouse с помощью Python



