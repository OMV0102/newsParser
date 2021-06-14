import requests
from datetime import datetime
from dataclasses import dataclass, field
import psycopg2
import sys

from natasha import Segmenter
from natasha import MorphVocab
from natasha import NewsEmbedding
from natasha import NewsMorphTagger
from natasha import NewsSyntaxParser
from natasha import NewsNERTagger
from natasha import NamesExtractor
from natasha import Doc


@dataclass
class NewsMember:
    """Структура найденного человека в новости"""
    idNews: str = ''
    idPerson: str = ''
    linkPerson: str = ''
    startPos: int = 0
    stopPos: int = 0
    nameNorm: str = ''
    surnameNorm: str = ''
    patronymicNorm: str = ''
    isFind: bool = False

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


# параметры подключения к БД
def getConnectionParametrs():
    pgDatabase = "news"
    pgUser = "newman"
    pgPassword = "newman"
    pgHost = "localhost"
    pgPort = "5432"

    return pgDatabase, pgUser, pgPassword, pgHost, pgPort

# соединение с БД Postgres
def getConnection(database, user, password, host, port):
    isGoodExecution = True
    conn = None
    try:
        conn = psycopg2.connect(
            database=database,
            user=user,
            password=password,
            host=host,
            port=port)
        return isGoodExecution, '', conn
    except Exception as errMessage:
        isGoodExecution = False
        return isGoodExecution, 'Не удалось подключиться к БД ' + database, conn

# получение новостей по апи в listNews
def getNewsFromApi(api, year):
    isGoodExecution = True
    listNews = []
    try:
        response = requests.get(api + year)  # получили данные
        if response.status_code == 200:
            jsonList = response.json()  # получили элементы в список
            if len(jsonList) > 0:
                for jsonElem in jsonList:
                    listElem = News(jsonElem['ID'], jsonElem['URL'], jsonElem['TITLE'], jsonElem['TEXT'],
                                    jsonElem['SHORTTEXT'], jsonElem['NEWS_DATE'], '', False, False, '')
                    listNews.append(listElem)

                return isGoodExecution, '', listNews

            else:
                raise ValueError('За ' + year + ' год новостей нет!')
        else:
            raise ValueError('Ошибка: api новостей вернуло код: ' + response.status_code)
    except Exception as errMessage:
        isGoodExecution = False
        return isGoodExecution, errMessage, listNews

# загрузка полученных новостей из listNews в БД
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
                data = (
                el.id, el.url, el.title_orig, el.text_orig, el.shorttext, el.news_date, el.text_parse, el.is_parse,
                el.is_fio,
                el.url, el.title_orig, el.text_orig, el.shorttext, el.news_date, str(datetime.now()))
                cur.execute(query, data)

            if (conn != None):
                conn.commit()
                conn.close()
            return isGoodExecution, ''

    except psycopg2.Error as errMessage:
        if (conn != None):
            conn.rollback()
            conn.close()
        isGoodExecution = False
        return isGoodExecution, 'Ошибка при загрузке полученных новостей в БД:\n' + errMessage.diag.message_primary + '\n' + errMessage.diag.message_detail

    except Exception as errMessage:
        if (conn != None):
            conn.rollback()
            conn.close()
        isGoodExecution = False
        return isGoodExecution, 'Ошибка при загрузке полученных новостей в БД:\n' + errMessage

# получение сотрудников по апи в listEmployee
def getEmployeesFromApi(api, header, key, link):
    isGoodExecution = True
    listEmployee = []
    try:
        response = requests.get(api, headers={header: key})
        if response.status_code == 200:

            jsonList = response.json()
            if len(jsonList) > 0:

                for jsonElem in jsonList:
                    patronymic = ''
                    if jsonElem['PATRONYMIC'] == None: patronymic = ''
                    else: patronymic = jsonElem['PATRONYMIC']

                    listElem = Employee(jsonElem['IDPERSON'], jsonElem['NAME'], jsonElem['SURNAME'],
                                        patronymic, link + str(jsonElem['IDPERSON']), jsonElem['POST'],
                                        jsonElem['CHAIR_ID'], jsonElem['CHAIR_ID2'], jsonElem['ID'], '')
                    listEmployee.append(listElem)

                return isGoodExecution, '', listEmployee

            else:
                raise ValueError('Список сотрудников, полученных по api, пуст!')
        else:
            raise ValueError('Ошибка: api сотрудников вернуло код: ' + response.status_code)

    except Exception as errMessage:
        isGoodExecution = False
        return isGoodExecution, errMessage, listEmployee

# загрузка полученных сотрудников из listEmployee в БД
def loadEmployeesToDatabase(listEmployee):
    isGoodExecution = True
    conn = None
    try:
        pgDatabase, pgUser, pgPassword, pgHost, pgPort = getConnectionParametrs()
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
                data = (
                el.idperson, el.name, el.surname, el.patronymic, el.link_person, el.post, el.chair_id, el.chair_id2,
                el.id,
                el.name, el.surname, el.patronymic, el.link_person, el.post, el.chair_id, el.chair_id2, el.id,
                str(datetime.now()))
                cur.execute(query, data)

            if (conn != None):
                conn.commit()
                conn.close()
            return isGoodExecution, ''

    except psycopg2.Error as errMessage:
        if (conn != None):
            conn.rollback()
            conn.close()
        isGoodExecution = False
        return isGoodExecution, 'Ошибка при загрузке полученных новостей в БД:\n' + errMessage.diag.message_primary + '\n' + errMessage.diag.message_detail

    except Exception as errMessage:
        if (conn != None):
            conn.rollback()
            conn.close()
        isGoodExecution = False
        return isGoodExecution, 'Ошибка при загрузке полученных новостей в БД:\n' + errMessage

# проверка ввода года новостей
def checkYearNews(year):

    return (year.isdigit() and int(year) >= 2007 and int(year) <= datetime.now().year)

# получение всех сотрудников из БД
def getEmployeesFromDb():
    isGoodExecution = True
    listEmployee = []
    conn = None
    try:
        pgDatabase, pgUser, pgPassword, pgHost, pgPort = getConnectionParametrs()
        isGoodExecution, message, conn = getConnection(pgDatabase, pgUser, pgPassword, pgHost, pgPort)
        if isGoodExecution == False:
            raise ValueError('Не удалось подключиться к БД: \n' + message)
        else:
            cur = conn.cursor()
            query = """
                    SELECT idperson, name, surname, patronymic, link_person, post, chair_id, chair_id2, id, update_ts
                    FROM public.teachers
                    ORDER BY surname, name, patronymic
                    ;"""
            cur.execute(query)
            responseList = cur.fetchall()
            for elem in responseList:
                listElem = Employee(int(elem[0]), elem[1], elem[2], elem[3], elem[4], elem[5], elem[6], elem[7], elem[8], str(elem[9]))
                listEmployee.append(listElem)

            if (conn != None):
                conn.commit()
                conn.close()
            return isGoodExecution, '', listEmployee

    except psycopg2.Error as errMessage:
        if (conn != None):
            conn.rollback()
            conn.close()
        isGoodExecution = False
        return isGoodExecution, 'Ошибка при загрузке всех сотрудников из БД для обработки новостей:\n' + errMessage.diag.message_primary, listEmployee

    except Exception as errMessage:
        if (conn != None):
            conn.rollback()
            conn.close()
        isGoodExecution = False
        return isGoodExecution, 'Ошибка при загрузке всех сотрудников из БД для обработки новостей:\n' + errMessage, listEmployee

# получение новостей из БД кроме уже обработанных
# ЛИМИТ 2 для отладки
# WHERE для отладки
def getNewsFromDbExceptParsed(year):
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
                    WHERE is_parse = false AND EXTRACT(YEAR FROM news_date)::text = %s
                    AND id = 132165
                    ORDER BY id
                    --LIMIT 5
                    ;"""
            data = (year, )
            cur.execute(query, data)
            responseList = cur.fetchall()
            for elem in responseList:
                listElem = News(int(elem[0]), elem[1], elem[2], elem[3], elem[4], str(elem[5]), elem[6], bool(elem[7]), bool(elem[8]), str(elem[9]))
                listNews.append(listElem)

            if (conn != None):
                conn.commit()
                conn.close()
            return isGoodExecution, '', listNews

    except psycopg2.Error as errMessage:
        if (conn != None):
            conn.rollback()
            conn.close()
        isGoodExecution = False
        return isGoodExecution, 'Ошибка при загрузке полученных новостей из БД для из обработки:\n' + errMessage.diag.message_primary, listNews

    except Exception as errMessage:
        if (conn != None):
            conn.rollback()
            conn.close()
        isGoodExecution = False
        return isGoodExecution, 'Ошибка при загрузке полученных новостей из БД для из обработки:\n' + errMessage, listNews

def findFioInNewsByNatasha(listNews, listEmployee):
    try:
        isGoodExecution = True
        listNewsMember = []

        # важные объекты для работы Наташи
        # удалить 1 строку - не будет работать
        segmenter = Segmenter()
        morph_vocab = MorphVocab()
        emb = NewsEmbedding()
        morph_tagger = NewsMorphTagger(emb)
        # syntax_parser = NewsSyntaxParser(emb) # not use
        ner_tagger = NewsNERTagger(emb)
        names_extractor = NamesExtractor(morph_vocab)

        for elemNews in listNews:
            listNewsMember.clear() #очищаем список
            flag_is_fio = False  # флаг, что изначально фио не найдены в документе
            # обрабатываем текст
            doc = Doc(elemNews.text_orig) # создаем объект из текста новости
            doc.segment(segmenter) # если убрать, то tag_ner выдаст исключение
            doc.tag_morph(morph_tagger) # если это не сделать, но нормализация не сработает
            # doc.parse_syntax(syntax_parser) # not use
            doc.tag_ner(ner_tagger) # разбивает документ на теги, чтобы появились spans в doc

            # смотрим распознанные сущности
            for span in doc.spans:
                # если сущность это человек (PERson)
                if span.type == 'PER':
                    span.normalize(morph_vocab) # нормализует фио в span.normal
                    span.extract_fact(names_extractor) # заполняет фио в span.fact по полям

                    # заполняем нормализованное фио, если чего-то, то ''
                    name = span.fact.as_dict.get('first', '')
                    surname = span.fact.as_dict.get('last', '')
                    patronymic = span.fact.as_dict.get('middle', '')

                    # запомнили всех людей в список
                    elemNewsMember = NewsMember(elemNews.id, 0, '', span.start, span.stop, name, surname, patronymic, False)
                    listNewsMember.append(elemNewsMember)

                    # ищем в сотрудниках и удаляем из списка найденных
                    listNewsMember = findEmployeeOnFio(listNewsMember, listEmployee)
                    flag_is_fio = True  # нашли хоть одно фио

                    print('\n')

            print('\n')
            return isGoodExecution, ''

    except Exception as errMessage:
        isGoodExecution = False
        return isGoodExecution, errMessage

# поиск человека в списке сотрудников по фамилии
def findPersonInlistEmployeeOnSurname(person, listEmployee):
    # возвращает индекс сотрудника в списке или -1
    start = 0
    end = len(listEmployee) - 1
    mid = (start + end) // 2

    while start <= end:
        mid = (start + end) // 2
        if person.surnameNorm.lower() < listEmployee[mid].surname.lower():
            end = mid - 1
        elif person.surnameNorm.lower() > listEmployee[mid].surname.lower():
            start = mid + 1
        elif person.surnameNorm.lower() == listEmployee[mid].surname.lower():
            return mid
    return -1

def findEmployeeOnFio(listNewsMember, listEmployee):
    # Обычным двоичным поиском ищем человека среди сотрудников

    for member in listNewsMember:
        if len(member.surnameNorm) > 0 and len(member.nameNorm) > 0:
            index = findPersonInlistEmployeeOnSurname(member,listEmployee)
            if index < 0:
                pass # ничего не делаем, т.к. не найден ни один сотрудник с такой фамилией
            else:
                # ====================================================================
                # если нашли человека с такой фамилией
                # идем влево и вправо по списку, вдруг есть однофамильцы
                listIndex = []
                flag = True
                # влево
                i = index - 1
                while (flag == True and index >= 0):
                    if (listEmployee[index].surname == listEmployee[i].surname):
                        listIndex.append(i)
                        index = index - 1
                    else:
                        flag = False
                listIndex.append(index) # между лево и право кладем сам index, чтобы по порядку
                # вправо
                flag = True
                i = index + 1
                n = len(listEmployee)
                while (flag == True and index < n):
                    if (listEmployee[index].surname == listEmployee[i].surname):
                        listIndex.append(i)
                        index = index + 1
                    else:
                        flag = False
                # =============================================================
                # теперь просматриваем сотрудников по индексу И
                # провереям совпадают ли хотябы первые буквы имен c именем в member, если нет индекс удаляем
                for i in listIndex:
                    if(member.nameNorm.lower()[0] != listEmployee[i].name.lower()[0]):
                        listIndex.remove(i)


                member.isFind = True
                member.idPerson = listEmployee[index].idperson
                member.linkPerson = listEmployee[index].link_person
        else:
            pass
            # тут когда фамилия или имя пустые
            # обрабатываем сразу отсеянных по найднным






def main():
    # Переменные
    apiNews = 'https://api.ciu.nstu.ru/v1.0/news/schoolkids/'  # апи новостей без года
    apiEmployee = "https://api.ciu.nstu.ru/v1.0/data/proj/teachers"  # апи сотрудников
    apiHeader = "Http-Api-Key"  # заголовок ключа запроса для апи сотрудников
    apiKey = "Y!@#13dft456DGWEv34g435f"  # ключ запроса для апи сотрудников
    linkPerson = "https://ciu.nstu.ru/kaf/persons/"  # ссылка на страницу сотрудника без id

    listNews = []  # сюда получаем новости с api
    listEmployee = []  # сюда получаем сотрудников с api
    errMessage = 'Сообщение'
    flagMenu = True

    while flagMenu == True:
        try:
            choice = -1
            print('\nМеню:')
            print('<1> Загрузить новости с api')
            print('<2> Загрузить сотрудников с api')
            print('<3> Обработать новости за выбранный год')

            choice = input('Выбор: ')
            if choice.isdigit(): choice = int(choice)
            else: choice = -1

            if choice == 1:
                year = input('Введите год, за который получить новости: ')
                if (checkYearNews(year)) == False:
                    raise ValueError('Год должен быть от 2007 по ' + str(datetime.now().year) + '!')
                else:
                    print('Загрузка началась...')
                    isGoodExecution, errMessage, listNews = getNewsFromApi(apiNews, year)
                    if isGoodExecution == False: raise ValueError(errMessage)
                    isGoodExecution, errMessage = loadNewsToDatabase(listNews)
                    if isGoodExecution == False: raise ValueError(errMessage)
                    print('Новости загружены.')

            elif choice == 2:
                print('Загрузка началась...')
                isGoodExecution, errMessage, listEmployee = getEmployeesFromApi(apiEmployee, apiHeader, apiKey, linkPerson)
                if isGoodExecution == False: raise ValueError(errMessage)
                isGoodExecution, errMessage = loadEmployeesToDatabase(listEmployee)
                if isGoodExecution == False: raise ValueError(errMessage)
                print('Сотрудники загружены.')

            elif choice == 3:
                year = '2021'
                year = input('Введите год, за который обработать новости: ')
                if (checkYearNews(year)) == False:
                    raise ValueError('Год должен быть от 2007 по ' + str(datetime.now().year) + '!')
                else:
                    print('Загружаем всех сотрудников...')
                    isGoodExecution, errMessage, listEmployee = getEmployeesFromDb()
                    if isGoodExecution == False: raise ValueError(errMessage)
                    print('Загружаем необработанные новости за ' + str(year) + ' год ...')
                    isGoodExecution, errMessage, listNews = getNewsFromDbExceptParsed(year)
                    if isGoodExecution == False: raise ValueError(errMessage)
                    print('Обрабатываем новости...')
                    isGoodExecution, errMessage = findFioInNewsByNatasha(listNews, listEmployee)
                    if isGoodExecution == False: raise ValueError(errMessage)
                    print('Загружаем в БД обработанные новости за ' + str(year) + ' год ...')

            elif choice == 4:
                print('4 НЕТ')

            elif choice == 0:
                flagMenu = False
                raise ValueError('Пользователь завершил работу программы.')

            else:
                raise ValueError('Действие не найдено.')


        except Exception as Message:
            print(Message)


if __name__ == '__main__':
    main()
