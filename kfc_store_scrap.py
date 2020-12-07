from requests import post
import pandas as pd
import json
import sqlite3

# Собираем запрос и подключаемся к Api
post_url = "https://api.kfc.com/api/store/v2/store.geo_search"
post_headers = {
    "authority" : "api.kfc.com",
    "method" : "POST",
    "path" : "/api/store/v2/store.geo_search",
    "scheme" : "https",
    "accept" : "application/json, text/plain, */*",
    "accept-encoding" : "gzip, deflate, br",
    "accept-language" : "ru-RU,ru;q : 0.9,en-US;q : 0.8,en;q : 0.7",
    "content-length" : "95",
    "origin" : "https://www.kfc.ru",
    "referer" : "https://www.kfc.ru/",
    "sec-fetch-dest" : "empty",
    "sec-fetch-mode" : "cors",
    "sec-fetch-site" : "cross-site",
    "user-agent" : "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/87.0.4280.88 Safari/537.36",
    "content-type" : "application/json"
}
post_data = '{"coordinates":[55.04141799999999,82.91274299999998],"radiusMeters":1000000000,"channel":"website"}'
request = post(post_url, data=post_data, headers=post_headers)


# Парсим ответ и собираем основную таблицу stores
response_json = json.loads(request.text)
json_data = response_json['searchResults']
stores = pd.json_normalize(json_data)


# Функция для парсинга вложенных json. На выходе из всех вложений столбца получается новая таблица
def parse_nested_json(dataframe, column_name):
    df_list = list()
    for rown in range(len(dataframe)):
        nested_dt = pd.json_normalize(dataframe[column_name][rown])
        nested_dt['store.storeId'] = dataframe['store.storeId'][rown]
        df_list.append(nested_dt)
    result_df = pd.concat(df_list, ignore_index=True)
    return result_df


# Парсим вложенные json
stores_landmarks = parse_nested_json(stores, 'store.contacts.navigationLandmarks')
stores_landmarks = stores_landmarks.drop(columns='landmarkName.en') # landmarkName.en - неполный дубликат landmarkName.ru
stores_landmarks = stores_landmarks.dropna(subset=['landmarkName.ru']) # Чистим от бесполезных строк

stores_services = parse_nested_json(stores, 'store.services')
stores_services = stores_services.drop(columns='availableNow') # availableNow - динмический показатель
stores_services = stores_services.dropna(subset=['availability.regular.startTimeLocal']) # Чистим от бесполезных строк

stores_menues = parse_nested_json(stores, 'store.menues')
stores_menues = stores_menues.drop(columns='availableNow') # availableNow - динмический показатель
stores_menues = stores_menues.dropna(subset=['availability.regular.startTimeLocal']) # Чистим от бесполезных строк


# Для основной таблицы отбираем стобцы с содержательной уникальной информацией
stores = stores[['store.storeId', 'store.title.en', 'store.title.ru', 'store.contacts.streetAddress.ru',
    'store.contacts.coordinates.geometry.coordinates', 'store.contacts.storeManager.ru',
    'store.contacts.phoneNumber', 'store.availableChannels', 'store.openingHours.regular.startTimeLocal',
    'store.openingHours.regular.endTimeLocal', 'store.status', 'store.features'
    ]]


# Функция парсинга вложенных листов - превращает их в буллевы флаги
def parse_nested_list(dataframe, column_name):
    d = list()

    for row in dataframe[column_name]:
        for el in row:
            if el not in d:
                d.append(el)

    for unel in d:
        dataframe[column_name + '.' + unel] = 0

    for rown in range(len(dataframe)):
        for upel in dataframe[column_name][rown]:
            dataframe.at[rown, column_name + '.' + upel] = 1


# Парсим вложенные листы
parse_nested_list(stores, 'store.features')
parse_nested_list(stores, 'store.availableChannels')


# Превращаем столбец координат в текстовый
stores['store.contacts.coordinates.geometry.coordinates'] = stores['store.contacts.coordinates.geometry.coordinates'].apply(str)


# Удаляем родительские столбцы листов
stores = stores.drop(columns= ['store.features', 'store.availableChannels'])


# Создаём БД и загружаем в неё все таблицы
conn = sqlite3.connect('./kfc.db')
stores.to_sql(name='stores', con=conn, if_exists='replace', index=False)
stores_landmarks.to_sql(name='stores_landmarks', con=conn, if_exists='replace', index=False)
stores_services.to_sql(name='stores_services', con=conn, if_exists='replace', index=False)
stores_menues.to_sql(name='stores_menues', con=conn, if_exists='replace', index=False)
conn.commit()


# Проверка запроса
conn.execute("""
    select *
    from stores st
    left join stores_menues mn
    on st."store.storeId" = mn."store.storeId"
    where
        st."store.title.ru" like '%Новосибирск%' and
        mn."name" = 'Завтрак' and
        '08:45:00' between mn."availability.regular.startTimeLocal" and mn."availability.regular.endTimeLocal"
""").fetchall()
