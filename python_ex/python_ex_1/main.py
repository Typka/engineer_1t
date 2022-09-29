# This is a sample Python script.

# Press Shift+F10 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.
import json
import requests as req
import tqdm
from bs4 import BeautifulSoup
import time
from requests_tor import RequestsTor
#req = RequestsTor(tor_ports=(9000,9001,9002,9003,9004),tor_cport=9151,password=None,
                  #autochange_id=5,threads=8,verbose=True)
#url = "https://hh.ru/"
#response = req.get(url)
#print(response)
sess = req.Session()
sess.headers.update({
    'location': 'https://hh.ru/',
    'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
    'accept-encoding': 'gzip, deflate, br',
    'accept-language': 'ru',
    'cache-control': 'no-cache',
    'pragma': 'no-cache',
    'sec-ch-ua': '".Not/A)Brand";v="99", "Google Chrome";v="103", "Chromium";v="103"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"Linux"',
    'sec-fetch-dest': 'document',
    'sec-fetch-mode': 'navigate',
    'sec-fetch-site': 'none',
    'sec-fetch-user': '?1',
    'upgrade-insecure-requests': '1',
    'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/103.0.0.0 Safari/537.36'
})

data = {
    "data":[]
}
url = "https://hh.ru/search/vacancy?search_field=name&search_field=company_name&search_field=description&text=Python&from=suggest_post"
response = sess.get(url)
soup = BeautifulSoup(response.text,"lxml")
max_page = soup.find_all(attrs={"data-qa":"pager-page"})[-1].text
tags = soup.find_all(attrs={"data-qa":"serp-item__title"})
for page in range(0,int(max_page)):
    url = f"https://hh.ru/search/vacancy?search_field=name&search_field=company_name&search_field=description&text=Python&from=suggest_post&page={page}&hhtmFrom=vacancy_search_list"
    response = sess.get(url)
    soup = BeautifulSoup(response.text, "lxml")
    tags = soup.find_all(attrs={"data-qa": "serp-item__title"})
    try:
        for iter in tqdm.tqdm(tags):
             #try:
                 response = sess.get(iter["href"])
                 soup_object = BeautifulSoup(response.text, "lxml")
                 try:
                     salary = soup_object.find(attrs={"data-qa": "vacancy-salary"}).text.replace(" ","")
                 except:
                     salary = None
                 try:
                     experience = soup_object.find(attrs={"data-qa":"vacancy-experience"}).text
                 except:
                     experience = None
                 try:
                     region_name = soup_object.find(class_="vacancy-creation-time-redesigned").text.split(" в ")[-1]
                 except:
                     region_name = None

                 data["data"].append({"title":iter.text,"work experience": experience, "salary":salary,"region":region_name})

                 with open("data.json", "w") as file:
                    json.dump(data, file, ensure_ascii=False)
             #except:
                 #continue
    except:
        continue
    print(len(data["data"]))
