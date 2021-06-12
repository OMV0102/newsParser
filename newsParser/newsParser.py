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

#параметры подключения к БД
def getConnectionParametrs():
    pgDatabase="news"
    pgUser="newman"
    pgPassword="newman"
    pgHost="localhost"
    pgPort="5432"

    return pgDatabase, pgUser, pgPassword, pgHost ,pgPort

#соеднинене с БД Postgres
def getConnection(database, user, password, host, port):
    isGoodExecution = True
    conn = None
    try:
        conn = psycopg2.connect(
            database=database, 
            user=user, 
            password=password, 
            host = host, 
            port = port)
        return isGoodExecution, '', conn
    except Exception as errMessage:
        isGoodExecution = False
        return isGoodExecution, 'Не удалось подключиться к БД ' + database, conn

# получение новостей по апи в listNews
def getNews(api, year):
    isGoodExecution = True
    listNews = []
    try:
        response = requests.get(api+year) # получили данные
        if response.status_code == 200:
            jsonList = response.json() # получили элементы в список
            if len(jsonList) > 0:
                for jsonElem in jsonList:
                    listElem = News(jsonElem['ID'], jsonElem['URL'], jsonElem['TITLE'], jsonElem['TEXT'], jsonElem['SHORTTEXT'], jsonElem['NEWS_DATE'])
                    listNews.append(listElem)

                return isGoodExecution, '', listNews

            else:
                raise ValueError('За ' + year + ' год новостей нет!')
        else:
            raise ValueError('Ошибка: api новостей вернуло код: ' + response.status_code)
    except Exception as errMessage:
        isGoodExecution = False
        return isGoodExecution, errMessage, listNews

#загрузка полученных новостей из listNews в БД
def loadNewsToDatabase(listNews):
    isGoodExecution = True
    conn = None
    num = 0
    try:
        pgDatabase, pgUser, pgPassword, pgHost, pgPort = getConnectionParametrs()
        isGoodExecution, message, conn = getConnection(pgDatabase, pgUser, pgPassword, pgHost, pgPort)
        if isGoodExecution == False:
            raise ValueError('Не удалось подключиться к БД: \n' + message)
        else:
            cur = conn.cursor()
            for el in listNews:
                query = """INSERT INTO public.news
                        (id, url, title_orig, text_orig, news_date)
                        VALUES
                        (%s, %s, %s, %s, %s::TIMESTAMP)
                        ON CONFLICT (id) DO NOTHING;"""
                data = (el.id, el.url, el.title_orig, el.text_orig, el.news_date)
                cur.execute(query, data)
                #conn.commit()

            if(conn != None):
                conn.rollback()
                conn.close()
            return isGoodExecution, ''

    except psycopg2.Error as errMessage:
        if(conn != None):
            conn.rollback()
            conn.close()
        isGoodExecution = False
        return isGoodExecution, 'Ошибка при загрузке полученных новостей в БД:\n' + errMessage.diag.message_primary

    except Exception as errMessage:
        if(conn != None):
            conn.rollback()
            conn.close()
        isGoodExecution = False
        return isGoodExecution, 'Ошибка при загрузке полученных новостей в БД:\n' + errMessage


# получение новостей по апи в listEmployee
def getEmployees(api, header, key):
    isGoodExecution = True
    listEmployee = []
    try:
        response = requests.get(api, headers={header:key})
        if response.status_code == 200:
            
            jsonList = response.json()
            if len(jsonList) > 0:

                for jsonElem in jsonList:
                    listElem = Employee(jsonElem['IDPERSON'], jsonElem['NAME'], jsonElem['SURNAME'], jsonElem['PATRONYMIC'], jsonElem['POST'], jsonElem['CHAIR_ID'], jsonElem['CHAIR_ID2'], jsonElem['ID'])
                    listEmployee.append(listElem)
                
                return isGoodExecution, '', listEmployee

            else:
                raise ValueError('Список сотрудников, полученных по api, пуст!')
        else:
            raise ValueError('Ошибка: api сотрудников вернуло код: ' + response.status_code)

    except Exception as errMessage:
        isGoodExecution = False
        return isGoodExecution, errMessage, listEmployee


#загрузка полученных новостей из listEmployee в БД
def loadEmployeesToDatabase(listEmployee, link):
    isGoodExecution = True
    conn = None
    try:
        pgDatabase, pgUser, pgPassword, pgHost ,pgPort = getConnectionParametrs()
        isGoodExecution, message, conn = getConnection(pgDatabase, pgUser, pgPassword, pgHost, pgPort)
        if isGoodExecution == False:
            raise ValueError('Не удалось подключиться к БД: \n' + message)
        else:
            cur = conn.cursor()
            for el in listEmployee:
                query = """INSERT INTO public.teachers
                        (idperson, name, surname, patronymic, link_person, post, chair_id, chair_id2, id)
                        VALUES
                        (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (idperson)
                        DO UPDATE SET
                        name = %s, surname = %s, patronymic = %s, link_person = %s, post = %s, chair_id = %s, chair_id2 = %s, id = %s;"""
                data = (el.idperson, el.name, el.surname, el.patronymic, link+str(el.idperson), el.post, el.chair_id, el.chair_id2, el.id,
                        el.name, el.surname, el.patronymic, link+str(el.idperson), el.post, el.chair_id, el.chair_id2, el.id)
                cur.execute(query, data)
                #conn.commit()
            if(conn != None):
                conn.rollback()
                conn.close()
            return isGoodExecution, ''

    except psycopg2.Error as errMessage:
        if(conn != None):
            conn.rollback()
            conn.close()
        isGoodExecution = False
        return isGoodExecution, 'Ошибка при загрузке полученных новостей в БД:\n' + errMessage.diag.message_primary

    except Exception as errMessage:
        if(conn != None):
            conn.rollback()
            conn.close()
        isGoodExecution = False
        return isGoodExecution, 'Ошибка при загрузке полученных новостей в БД:\n' + errMessage



def main():
    # Переменные
    apiNews = 'https://api.ciu.nstu.ru/v1.0/news/schoolkids/' # апи новостей без года
    newsYear = '2021'
    apiEmployee = "https://api.ciu.nstu.ru/v1.0/data/proj/teachers" # апи сотрудников
    apiHeader = "Http-Api-Key" # заголовок ключа запроса для апи сотрудников
    apiKey = "Y!@#13dft456DGWEv34g435f" # ключ запроса для апи сотрудников
    linkPerson = "https://ciu.nstu.ru/kaf/persons/" # ссылка на страницу сотрудника без id

    listNews = [] # сюда получаем новости с api
    listEmployee = [] # сюда получаем сотрудников с api
    errMessage = 'Сообщение'

    try:
        isGoodExecution, errMessage, listNews = getNews(apiNews, newsYear)
        if isGoodExecution == False: raise ValueError(errMessage)
        isGoodExecution, errMessage = loadNewsToDatabase(listNews)
        if isGoodExecution == False: raise ValueError(errMessage)
        isGoodExecution, errMessage, listEmployee = getEmployees(apiEmployee, apiHeader, apiKey)
        if isGoodExecution == False: raise ValueError(errMessage)
        isGoodExecution, errMessage = loadEmployeesToDatabase(listEmployee, linkPerson)
        if isGoodExecution == False: raise ValueError(errMessage)




    except Exception as Message:
        print(Message)

main()
