# -*- coding: utf-8 -*-
"""
Created on Wed Jan  29 15:00:20 2022

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

#Получаем ответ по api, и переводим его в кодировку utf-8
response = requests.get("https://www.vedomosti.ru/rss/news")
response.encoding = response.apparent_encoding

#Скармливаем данные bs4 для удобного парсинга html страницы
soup = BeautifulSoup(response.text,features="xml")

#Находим тег item и в нем ищем теги , приведенные ниже, помещаем их в массив, для того чтобы в дальнейшем сделать dataframe
title = []
link = []
guid = []
pdalink = []
category = []
pubDate = []
for child in soup.find_all("item"):
    title.append(child.find("title"))
    link.append(child.find("link"))
    guid.append(child.find("guid"))
    pdalink.append(child.find("pdalink"))
    category.append(child.find("category"))
    pubDate.append(child.find("pubDate"))

#Создаем dataframe
columns = ['title','link','guid','pdalink','category','pubDate']
df = pd.DataFrame(zip(title,link,guid,pdalink,category,pubDate),
                  columns = columns)

#Так как данные у нас в массивах, убираем квадратные скобки
df = df.explode(['title','link','guid','pdalink','category','pubDate'])

#Производим нормализация и очистку данных, для дальнейшей выгрузки в базу
df['pubDate'] = pd.to_datetime(df['pubDate']).dt.date
df['category'] = df['category'].replace("Бизнес","Экономика").replace("Бизнес / Транспорт","Экономика")\
                               .replace("Финансы","Экономика").replace("Недвижимость","Общество")

#Выгружаем наш сырой файл в synology drive. Имя файла задаем время выполнения скрипта
out_file = io.BytesIO(soup.encode('utf-8'))
out_file.name = f'{datetime.now()}_vedomosti.txt'
with SynologyDrive(NAS_USER, NAS_PASS, NAS_IP, dsm_version='7') as synd:
    synd.upload_file(out_file, '/team-folders/Contragents/тестовая папка/test_api/Vedomosti raw')

#Передаем файл в формате csv в nifi
output = StringIO()
df.to_csv(output,index = False, quoting=csv.QUOTE_NONNUMERIC, encoding='UTF-8')
print(output.getvalue())