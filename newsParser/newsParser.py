import requests
from datetime import datetime
from dataclasses import dataclass
import psycopg2
import sys

from natasha import (
    Segmenter,
    MorphVocab,
    NewsEmbedding,
    NewsMorphTagger,
    NewsSyntaxParser,
    NewsNERTagger,
    PER,
    NamesExtractor,
    Doc
)

@dataclass
class News:
    """Структура новости"""
    id: int = 0
    url: str = ''
    title_orig: str = ''
    text_orig: str = ''
    shorttext: str = ''
    news_date: str = ''
    text_parse: str = ''
    is_parse: bool = False
    is_fio: bool = False
    update_ts: str = ''

@dataclass
class Employee:
    """Структура сотрудника"""
    idperson: int = 0
    name: str = ''
    surname: str = ''
    patronymic: str = ''
    link_person: str = ''
    post: str = ''
    chair_id: int = 0
    chair_id2: int = 0
    id: int = 0
    update_ts: str = ''

@dataclass
class newsMembers:
    idNews: str = ''
    idPerson: str = ''
    startPos: int = 0
    stopPos: int = 0

#параметры подключения к БД
def getConnectionParametrs():
    pgDatabase="news"
    pgUser="newman"
    pgPassword="newman"
    pgHost="localhost"
    pgPort="5432"

    return pgDatabase, pgUser, pgPassword, pgHost ,pgPort

#соединение с БД Postgres
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
def getNewsFromApi(api, year):
    isGoodExecution = True
    listNews = []
    try:
        response = requests.get(api+year) # получили данные
        if response.status_code == 200:
            jsonList = response.json() # получили элементы в список
            if len(jsonList) > 0:
                for jsonElem in jsonList:
                    listElem = News(jsonElem['ID'], jsonElem['URL'], jsonElem['TITLE'], jsonElem['TEXT'], jsonElem['SHORTTEXT'], jsonElem['NEWS_DATE'], '', False, False, '')
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
    try:
        pgDatabase, pgUser, pgPassword, pgHost, pgPort = getConnectionParametrs()
        isGoodExecution, message, conn = getConnection(pgDatabase, pgUser, pgPassword, pgHost, pgPort)
        if isGoodExecution == False:
            raise ValueError('Не удалось подключиться к БД: \n' + message)
        else:
            cur = conn.cursor()
            for el in listNews:
                query = """INSERT INTO public.news
                        (id, url, title_orig, text_orig, shorttext, news_date, text_parse, is_parse, is_fio, update_ts)
                        VALUES
                        (%s, %s, %s, %s, %s, %s::TIMESTAMP, %s, %s, %s, DEFAULT)
                        ON CONFLICT (id)
                        WHERE is_parse = false
                        DO UPDATE SET
                        url = %s, title_orig = %s, text_orig = %s, shorttext = %s, news_date = %s::TIMESTAMP, update_ts = %s::TIMESTAMP;"""
                data = (el.id, el.url, el.title_orig, el.text_orig, el.shorttext, el.news_date, el.text_parse, el.is_parse, el.is_fio,
                               el.url, el.title_orig, el.text_orig, el.shorttext, el.news_date, str(datetime.now()))
                cur.execute(query, data)

            if(conn != None):
                conn.commit()
                conn.close()
            return isGoodExecution, ''

    except psycopg2.Error as errMessage:
        if(conn != None):
            conn.rollback()
            conn.close()
        isGoodExecution = False
        return isGoodExecution, 'Ошибка при загрузке полученных новостей в БД:\n' + errMessage.diag.message_primary + '\n' + errMessage.diag.message_detail

    except Exception as errMessage:
        if(conn != None):
            conn.rollback()
            conn.close()
        isGoodExecution = False
        return isGoodExecution, 'Ошибка при загрузке полученных новостей в БД:\n' + errMessage

# получение сотрудников по апи в listEmployee
def getEmployeesFromApi(api, header, key, link):
    isGoodExecution = True
    listEmployee = []
    try:
        response = requests.get(api, headers={header:key})
        if response.status_code == 200:
            
            jsonList = response.json()
            if len(jsonList) > 0:

                for jsonElem in jsonList:
                    listElem = Employee(jsonElem['IDPERSON'], jsonElem['NAME'], jsonElem['SURNAME'], jsonElem['PATRONYMIC'], link + str(jsonElem['IDPERSON']), jsonElem['POST'], jsonElem['CHAIR_ID'], jsonElem['CHAIR_ID2'], jsonElem['ID'], '')
                    listEmployee.append(listElem)
                
                return isGoodExecution, '', listEmployee

            else:
                raise ValueError('Список сотрудников, полученных по api, пуст!')
        else:
            raise ValueError('Ошибка: api сотрудников вернуло код: ' + response.status_code)

    except Exception as errMessage:
        isGoodExecution = False
        return isGoodExecution, errMessage, listEmployee

#загрузка полученных сотрудников из listEmployee в БД
def loadEmployeesToDatabase(listEmployee):
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
                        (idperson, name, surname, patronymic, link_person, post, chair_id, chair_id2, id, update_ts)
                        VALUES
                        (%s, %s, %s, %s, %s, %s, %s, %s, %s, DEFAULT)
                        ON CONFLICT (idperson)
                        DO UPDATE SET
                        name = %s, surname = %s, patronymic = %s, link_person = %s, post = %s, chair_id = %s, chair_id2 = %s, id = %s, update_ts = %s::TIMESTAMP;"""
                data = (el.idperson, el.name, el.surname, el.patronymic, el.link_person, el.post, el.chair_id, el.chair_id2, el.id,
                                     el.name, el.surname, el.patronymic, el.link_person, el.post, el.chair_id, el.chair_id2, el.id, str(datetime.now()))
                cur.execute(query, data)

            if(conn != None):
                conn.commit()
                conn.close()
            return isGoodExecution, ''

    except psycopg2.Error as errMessage:
        if(conn != None):
            conn.rollback()
            conn.close()
        isGoodExecution = False
        return isGoodExecution, 'Ошибка при загрузке полученных новостей в БД:\n' + errMessage.diag.message_primary + '\n' + errMessage.diag.message_detail

    except Exception as errMessage:
        if(conn != None):
            conn.rollback()
            conn.close()
        isGoodExecution = False
        return isGoodExecution, 'Ошибка при загрузке полученных новостей в БД:\n' + errMessage

#проверка ввода года новостей
def checkYearNews(year):
    return (year.isdigit() and int(year) >= 2007 and int(year) <= datetime.now().year)

#получение новостей из БД кроме уже обработанных
#ЛИМИТ 2 для отладки
#WHERE для отладки
def getNewsFromDbExceptParsed():
    isGoodExecution = True
    listNews = []
    conn = None
    try:
        pgDatabase, pgUser, pgPassword, pgHost, pgPort = getConnectionParametrs()
        isGoodExecution, message, conn = getConnection(pgDatabase, pgUser, pgPassword, pgHost, pgPort)
        if isGoodExecution == False:
            raise ValueError('Не удалось подключиться к БД: \n' + message)
        else:
            cur = conn.cursor()
            query = """
                    SELECT id, url, title_orig, text_orig, shorttext, news_date, text_parse, is_parse, is_fio, update_ts
                    FROM public.news
                    WHERE is_parse = false
                    AND id = 132165
                    --LIMIT 2
                    ;"""
            cur.execute(query)
            responseList = cur.fetchall()
            for elem in responseList:
                listElem = News(int(elem[0]), elem[1], elem[2], elem[3], elem[4], str(elem[5]), elem[6], bool(elem[7]), bool(elem[8]), str(elem[9]))
                listNews.append(listElem)

            if(conn != None):
                conn.commit()
                conn.close()
            return isGoodExecution, '', listNews

    except psycopg2.Error as errMessage:
        if(conn != None):
            conn.rollback()
            conn.close()
        isGoodExecution = False
        return isGoodExecution, 'Ошибка при загрузке полученных новостей в БД:\n' + errMessage.diag.message_primary, listNews

    except Exception as errMessage:
        if(conn != None):
            conn.rollback()
            conn.close()
        isGoodExecution = False
        return isGoodExecution, 'Ошибка при загрузке полученных новостей в БД:\n' + errMessage, listNews


def findFioInNewsByNatasha(listNews):

    isGoodExecution = True
    errMessage = ''

    try:
        segmenter = Segmenter()
        morph_vocab = MorphVocab()

        emb = NewsEmbedding()
        morph_tagger = NewsMorphTagger(emb)
        syntax_parser = NewsSyntaxParser(emb)
        ner_tagger = NewsNERTagger(emb)

        names_extractor = NamesExtractor(morph_vocab)


        for elem in listNews:
            print('')
            doc = Doc(elem.text_orig)
            doc.segment(segmenter)
            doc.tag_morph(morph_tagger)
            doc.parse_syntax(syntax_parser)
            doc.tag_ner(ner_tagger)

            # после сегментера
            print(doc.tokens[:5])

            print('\n')
            print('\n')
            # после сегментера
            print(doc.sents[:5])

            # после таг морфы
            print(doc.tokens[:5])

            for span in doc.spans:
                span.normalize(morph_vocab)

            print('\n')
            print('\n')
            print('\n')
            print('\n')
            print('\n')
            print('\n')
            return isGoodExecution, ''

    except Exception as Message:
        return isGoodExecution, Message


def main():
    # Переменные
    apiNews = 'https://api.ciu.nstu.ru/v1.0/news/schoolkids/' # апи новостей без года
    newsYear = str(datetime.now().year)
    apiEmployee = "https://api.ciu.nstu.ru/v1.0/data/proj/teachers" # апи сотрудников
    apiHeader = "Http-Api-Key" # заголовок ключа запроса для апи сотрудников
    apiKey = "Y!@#13dft456DGWEv34g435f" # ключ запроса для апи сотрудников
    linkPerson = "https://ciu.nstu.ru/kaf/persons/" # ссылка на страницу сотрудника без id

    listNews = [] # сюда получаем новости с api
    listEmployee = [] # сюда получаем сотрудников с api
    errMessage = 'Сообщение'

    try:
        choice = 3
        #print('Меню:\n')
        #print('<1> Загрузить новости с api')
        #print('<2> Загрузить сотрудников с api')
        #print('<3> Обработать новости за выбранный год')

        #choice = input('Выбор: ')
        #if choice.isdigit(): choice = int(choice)
        #else: choice = -1

        if choice == 1:
            year = input('Введите год, за который получить новости: ')
            if (checkYearNews(year)) == False:
                raise ValueError('Год должен быть от 2007 по ' + str(datetime.now().year) + '!')
            else:
                newsYear = year
                isGoodExecution, errMessage, listNews = getNewsFromApi(apiNews, newsYear)
                if isGoodExecution == False: raise ValueError(errMessage)
                isGoodExecution, errMessage = loadNewsToDatabase(listNews)
                if isGoodExecution == False: raise ValueError(errMessage)
                print('Новости загружены.')

        elif choice == 2:
            isGoodExecution, errMessage, listEmployee = getEmployeesFromApi(apiEmployee, apiHeader, apiKey, linkPerson)
            if isGoodExecution == False: raise ValueError(errMessage)
            isGoodExecution, errMessage = loadEmployeesToDatabase(listEmployee)
            if isGoodExecution == False: raise ValueError(errMessage)
            print('Сотрудники загружены.')

        elif choice == 3:
            isGoodExecution, errMessage, listNews = getNewsFromDbExceptParsed()
            if isGoodExecution == False: raise ValueError(errMessage)
            isGoodExecution, errMessage = findFioInNewsByNatasha(listNews)
            if isGoodExecution == False: raise ValueError(errMessage)

        elif choice == 4:
            print('4 НЕТ')

        elif choice == 0:
            raise ValueError('Пользователь завершил работу программы.')

        else:
            raise ValueError('Действие не найдено.\nЗавершение работы...')



        return ''

    except Exception as Message:
        return Message

print(main())
