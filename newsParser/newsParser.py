import requests
from datetime import datetime
from dataclasses import dataclass
import psycopg2
import sys 

# Переменные
apiNews = 'https://api.ciu.nstu.ru/v1.0/news/schoolkids/' # апи новостей без года
apiEmployee = "https://api.ciu.nstu.ru/v1.0/data/proj/teachers" # апи сотрудников
apiHeader = "Http-Api-Key" # заголовок ключа запроса для апи сотрудников
apiKey = "Y!@#13dft456DGWEv34g435f" # ключ запроса для апи сотрудников
linkPerson = "https://ciu.nstu.ru/kaf/persons/" # ссылка на страницу сотрудника без id
newsYear = '2021'

listNews = []
listEmployee = []

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
class Employee:
    """Структура сотрудника"""
    idperson: int = 0
    name: str = ''
    surname: str = ''
    patronymic: str = ''
    post: str = ''
    chair_id: int = 0
    chair_id2: int = 0
    id: int = 0

#соеднинене с БД Postgres
def getConnection():
    conn = psycopg2.connect(
        database="news", 
        user="newman", 
        password="newman", 
        host="localhost", 
        port="5432")
    return conn

# получение новостей по апи в listNews
def getNews(api, year):
    response = requests.get(api+year)
    if response.status_code == 200:
        jsonList = response.json()
        if len(jsonList) > 0:
            for jsonElem in jsonList:
                listElem = News(jsonElem['ID'], jsonElem['URL'], jsonElem['TITLE'], jsonElem['TEXT'], jsonElem['SHORTTEXT'], jsonElem['NEWS_DATE'])
                listNews.append(listElem)
            return listNews
        else:
            print('За ' + year + ' год новостей нет!')
            return
    else:
        print('Ошибка: api новостей вернуло код: ' + response.status_code)
        return

#загрузка полученных новостей из listNews в БД
def loadNewsToDatabase(listNews):
    if len(listNews) > 0:
        conn = getConnection()
        cur = conn.cursor()
        for el in listNews:
            query = "INSERT INTO public.news (id, url, title_orig, text_orig, news_date) VALUES (%s, %s, %s, %s, %s::TIMESTAMP)"
            data = (el.id, el.url, el.title_orig, el.text_orig, el.news_date)
            cur.execute(query, data)
            conn.commit()
        conn.close()
    else:
        print('Список сотрудников пуст: в базу данных загружать не буду')
        return

# получение новостей по апи в listEmployee
def getEmployees(api, header, key):
    response = requests.get(api, headers={header:key})
    if response.status_code == 200:
        jsonList = response.json()
        if len(jsonList) > 0:
            for jsonElem in jsonList:
                listElem = Employee(jsonElem['IDPERSON'], jsonElem['NAME'], jsonElem['SURNAME'], jsonElem['PATRONYMIC'], jsonElem['POST'], jsonElem['CHAIR_ID'], jsonElem['CHAIR_ID2'], jsonElem['ID'])
                listEmployee.append(listElem)
        else:
            print('Список сотрудников, полученных по api, пуст!')
            return
    else:
        print('Ошибка: api сотрудников вернуло код: ' + response.status_code)
        return


#загрузка полученных новостей из listEmployee в БД
def loadEmployeesToDatabase(listEmployee, link):
    if len(listEmployee) > 0:
        conn = getConnection()
        cur = conn.cursor()
        for el in listEmployee:
            query = "INSERT INTO public.teachers (idperson, name, surname, patronymic, link_person, post, chair_id, chair_id2, id) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)"
            data = (el.idperson, el.name, el.surname, el.patronymic, link+str(el.idperson), el.post, el.chair_id, el.chair_id2, el.id)
            cur.execute(query, data)
            conn.commit()
        conn.close()
    else:
        print('Список новостей пустой: в базу данных загружать не буду')
        return


def main():
    getNews(apiNews, newsYear)
    loadNewsToDatabase(listNews)
    getEmployees(apiEmployee, apiHeader, apiKey)
    loadEmployeesToDatabase(listEmployee, linkPerson)


if __name__ == "__main__":
    main()
