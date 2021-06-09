import requests
from datetime import datetime
from dataclasses import dataclass
import psycopg2
import sys 

@dataclass
class News:
    """Структура новости"""
    id: int
    url: str
    title_orig: str
    text_orig: str
    shorttext: str
    news_date: str

@dataclass
class teachers:
    """Структура сотрудника"""
    idperson: int = 0
    name: str = ''
    surname: str = ''
    patronymic: str = ''
    post: str = ''
    chair_id: int = 0
    chair_id2: int = 0
    id: int = 0
    create_ts: datetime = datetime.now()



# Переменные
apiNews = 'https://api.ciu.nstu.ru/v1.0/news/schoolkids/' # апи новостей без года
apiEmployee = "https://api.ciu.nstu.ru/v1.0/data/proj/teachers" # апи сотрудников
apiHeader = "Http-Api-Key" # заголовок ключа запроса для апи сотрудников
apiKey = "Y!@#13dft456DGWEv34g435f" # ключ запроса для апи сотрудников
linkPerson = "https://ciu.nstu.ru/kaf/persons/" # ссылка на страницу сотрудника без id
newsYear = '2021'

listNews = []

response = requests.get(apiNews+newsYear)
if response.status_code == 200:
    jsonList = response.json()
    if len(jsonList) > 0:
        for jsonElem in jsonList:
            listElem = News(jsonElem['ID'], jsonElem['URL'], jsonElem['TITLE'], jsonElem['TEXT'], jsonElem['SHORTTEXT'], jsonElem['NEWS_DATE'])
            listNews.append(listElem)
    else:
        print('За ' + newsYear + ' год новостей нет!')
        sys.exit(1)
else:
    print('Ошибка api новостей вернуло код: ' + response.status_code)
    sys.exit(1)


conn = psycopg2.connect(
    database="news", 
    user="newman", 
    password="newman", 
    host="localhost", 
    port="5432"
)


cur = conn.cursor()
for el in listNews:
    query = "INSERT INTO public.news (id, url, title_orig, text_orig, news_date) VALUES (%s, %s, %s, %s, %s::TIMESTAMP)"
    data = (int(el.id), el.url, el.title_orig, el.text_orig, el.news_date)
    cur.execute(query, data)
    conn.commit()


conn.close()


