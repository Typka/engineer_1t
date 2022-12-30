# -*- coding: utf-8 -*-
"""
Created on Wed Jan  29 12:49:03 2022

@author: Ya.Brovkin
"""
import requests
import pandas as pd
import io
import configparser
from synology_drive_api.drive import SynologyDrive
from bs4 import BeautifulSoup
from io import StringIO
import csv
from datetime import datetime

#Подключаемся к synology drive
config = configparser.ConfigParser()
config.read(r'''D:\python_scripts\global_config.cfg''')
NAS_USER = str(config['SYNOLOGY']['username'])
NAS_PASS = str(config['SYNOLOGY']['password'])
NAS_IP = str(config['SYNOLOGY']['address'])

#Получаем ответ по api
response = requests.get("https://lenta.ru/rss/")

#Скармливаем данные bs4 для удобного парсинга html страницы
soup = BeautifulSoup(response.text,features="xml")

#Находим тег item и в нем ищем теги , приведенные ниже, помещаем их в массив, для того чтобы в дальнейшем сделать dataframe
guid = []
author = []
title = []
description = []
pubDate = []
category = []
for child in soup.find_all("item"):
    guid.append(child.find("guid"))
    author.append(child.find("author"))
    title.append(child.find("title"))
    description.append(child.find("description"))
    pubDate.append(child.find("pubDate"))
    category.append(child.find("category"))

#Создаем dataframe
columns = ['guid','author','title','description','pubDate','category']
df = pd.DataFrame(zip(guid,author,title,description,pubDate,category),
                  columns = columns)

#Так как данные у нас в массивах, убираем квадратные скобки
df = df.explode(['guid','author','title','description','pubDate','category'])

#Производим нормализация и очистку данных, для дальнейшей выгрузки в базу
df['description'] = df['description'].str.replace("\n    ",'')
df['pubDate'] = df['pubDate'].apply(lambda x: str(x))
df['pubDate'] = pd.to_datetime(df['pubDate']).dt.date
df['category'] = df['category'].replace("Наука и техника","Технологии").replace("Бывший СССР","Политика")\
                               .replace("Забота о себе","Общество").replace("Из жизни","Общество")\
                               .replace("Культура","Общество").replace("Интернет и СМИ","Медиа")\
                               .replace("Ценности","Общество").replace("Среда обитания","Общество")\
                               .replace("Силовые структуры","Общество").replace("Россия","Общество")\
                               .replace("Россия","Общество").replace("Путешествия","Общество")

#Выгружаем наш сырой файл в synology drive. Имя файла задаем время выполнения скрипта
out_file = io.BytesIO(soup.encode('utf-8'))
out_file.name = f'{datetime.now()}_lenta.txt'
with SynologyDrive(NAS_USER, NAS_PASS, NAS_IP, dsm_version='7') as synd:
    synd.upload_file(out_file, '/team-folders/Contragents/тестовая папка/test_api/Lenta raw')

#Передаем файл в формате csv в nifi
output = StringIO()
df.to_csv(output,index = False, quoting=csv.QUOTE_NONNUMERIC, encoding='UTF-8')
print(output.getvalue())