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
    idPerson: str = ''
    linkPerson: str = ''
    startPos: int = 0
    stopPos: int = 0
    nameNorm: str = ''
    surnameNorm: str = ''
    patronymicNorm: str = ''
    isFind: bool = False

@dataclass
class NewsParsed:
    listMembers: list[NewsMember]
    idNews: int = 0
    isFio: bool = False
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
# LIMIT  для отладки
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
                    --AND id = 132165
                    AND id = 132003
                    ORDER BY id
                    --LIMIT 5
                    ;"""
            data = (year, )
            cur.execute(query, data)
            responseList = cur.fetchall()
            for elem in responseList:
                listElem = News(int(elem[0]), elem[1], elem[2], elem[3], elem[4], str(elem[5]), elem[6], bool(elem[7]), bool(elem[8]), str(elem[9]))
                listNews.append(listElem)

            if (len(listNews) == 0):
                raise ValueError('Новостей за ' + year + ' год в БД не найдено!')

            if (conn != None):
                conn.commit()
                conn.close()

            return isGoodExecution, '', listNews

    except psycopg2.Error as errMessage:
        if (conn != None):
            conn.rollback()
            conn.close()
        isGoodExecution = False
        return isGoodExecution, 'Ошибка при загрузке полученных новостей из БД для из обработки: ' + errMessage.diag.message_primary, listNews

    except Exception as errMessage:
        if (conn != None):
            conn.rollback()
            conn.close()
        isGoodExecution = False
        return isGoodExecution, 'Ошибка при загрузке полученных новостей из БД для из обработки: ' + str(errMessage), listNews

# функция распознаем фио в списке новостей
def findFioInNewsByNatasha(listNews):
    try:
        isGoodExecution = True
        listNewsMember = []
        listNewsParsed = []

        # важные объекты для работы Наташи
        # удалить одну строку - не будет работать
        segmenter = Segmenter()
        morph_vocab = MorphVocab()
        emb = NewsEmbedding()
        morph_tagger = NewsMorphTagger(emb)
        # syntax_parser = NewsSyntaxParser(emb) # not use
        ner_tagger = NewsNERTagger(emb)
        names_extractor = NamesExtractor(morph_vocab)

        for elemNews in listNews:
            listNewsMember.clear()
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

                    # заполняем нормализованное фио, если чего-то нет, то пустая строка
                    name = span.fact.as_dict.get('first', '')
                    surname = span.fact.as_dict.get('last', '')
                    patronymic = span.fact.as_dict.get('middle', '')

                    # запомнили всех распознанных людей в список
                    elemNewsMember = NewsMember(0, '', span.start, span.stop, name, surname, patronymic, False)
                    listNewsMember.append(elemNewsMember)
            # если Наташа никого не нашла, то ставим флаг False
            if len(listNewsMember) > 0:
                elemNewsParsed = NewsParsed(listNewsMember, elemNews.id, True)
            else:
                elemNewsParsed = NewsParsed(listNewsMember, elemNews.id, False)
            listNewsParsed.append(elemNewsParsed)
            print('')

        return isGoodExecution, '', listNewsParsed

    except Exception as errMessage:
        isGoodExecution = False
        return isGoodExecution, 'Ошибка при распозновании фио в новостях:\n' + errMessage, listNewsParsed

# двоичный поиск фамилии в списке сотрудников
def binarySearchSurnameInListEmployee(member, listEmployee):
    # Обычным двоичным поиском ищем человека среди сотрудников
    # возвращает индекс сотрудника в списке или -1
    start = 0
    end = len(listEmployee) - 1

    while start <= end:
        mid = (start + end) // 2
        if member.surnameNorm.lower() < listEmployee[mid].surname.lower():
            end = mid - 1
        elif member.surnameNorm.lower() > listEmployee[mid].surname.lower():
            start = mid + 1
        elif member.surnameNorm.lower() == listEmployee[mid].surname.lower():
            return mid

    return -1

# сопоставление сотрудника распознанным фио в новости
def findPersonInlistEmployeeOnSurname(listNewsParsed, listEmployee):
    try:
        isGoodExecution = True

        for elemNewsParsed in listNewsParsed:
            # если список найденных не пустой, то есть isFio == True
            if elemNewsParsed.isFio == True:
                # ===============================================ЧАСТЬ 1==========================================================
                # в части 1 ищем совпадения по полному совпадению ФИО среди всех сотрудников
                for member in elemNewsParsed.listMembers:
                    if len(member.surnameNorm) > 1 and len(member.nameNorm) > 1 and len(member.patronymicNorm) > 1:
                        index = binarySearchSurnameInListEmployee(member,listEmployee)
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
                                    i = i - 1
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
                                    i = i + 1
                                else:
                                    flag = False
                            # =============================================================
                            # теперь просматриваем сотрудников по индексу И
                            # провереям совпадают ли имена c именем в member, если нет - индекс удаляем
                            i = 0
                            while i < len(listIndex):
                                if(member.nameNorm.lower() != listEmployee[listIndex[i]].name.lower()):
                                    listIndex.pop(i)
                                    i = i - 1
                                i = i + 1
                            # ====================================================================
                            # теперь смотрим остались ли вообще ещё кандидаты в listIndex
                            if len(listIndex) == 0:
                                pass # ниче не делаем, а значит в цикле к след. member переходит
                            else:
                                # если остались, то сравниваем отчества
                                i = 0
                                while i < len(listIndex):
                                    # если очества не совпали индекс удаляем
                                    if (member.patronymicNorm.lower() != listEmployee[listIndex[i]].patronymic.lower()):
                                        listIndex.pop(i)
                                        i = i - 1
                                    i = i + 1
                            # ======================================================================
                            # опять смотрим остались ли вообще ещё кандидаты в listIndex
                            if len(listIndex) == 0:
                                pass # если пусто, ниче не делаем, а значит в цикле к след. member переходит
                            elif len(listIndex) == 1:
                                member.isFind = True # сответствие нашли по полному фио
                                member.idPerson = listEmployee[listIndex[0]].idperson # запомнили id
                                member.linkPerson = listEmployee[listIndex[0]].link_person  # запомнили ссылку сотрудника
                            elif len(listIndex) > 1:
                                pass
                                # тут ситуация когда остались индексы тех, у кого полное совпадение по фио
                                # индексы лежат в listIndex
                                # что делать - не знаю, поэтому ничего не делаем
                                # но по хорошему нужно давать выбор, если это обработка при регистрации новой новости

                # ==============================================ЧАСТЬ 2=============================================================
                # часть 2 ищем совпадения по (Фамилия И.О.) и (Фамилия Имя) СРЕДИ распознанных по полному совпалению ФИО
                # когда уже пробежались по всем людям из новости, смотрим есть ли хотябы один найденный
                flagFind = False
                for elem in elemNewsParsed.listMembers:
                    if(elem.isFind == True):
                        flagFind = True

                # если совсем ни одного не смогли найти по полному фио, то ставим в новости что  isFio = False и всё  к след. новости
                if (flagFind == False):
                    elemNewsParsed.isFio = False
                else:
                    # =============================================================================================
                    # иначе ставим, что хоть кто-то да найден
                    elemNewsParsed.isFio = True
                    # теперь будем пытаться искать ненайденных по уже найденным
                    # =============================================================================================
                    # рассматриваем случай, когда человека упомянули по тексту как (Фамилия И.О.)
                    for elem1 in elemNewsParsed.listMembers:
                        # нераспознанный с инициалами
                        if (elem1.isFind == False and len(elem1.surnameNorm) > 1 and len(elem1.nameNorm) > 0 and len(elem1.patronymicNorm) > 0):
                            # нашли нераспознанного
                            for elem2 in elemNewsParsed.listMembers:
                                # ищем среди распознанных по полному фио
                                if (elem2.isFind == True):
                                    if (elem1.surnameNorm.lower() == elem2.surnameNorm.lower() and elem1.nameNorm.lower()[0] == elem2.nameNorm.lower()[0] and elem1.patronymicNorm.lower()[0] == elem2.patronymicNorm.lower()[0]):
                                        # если вдруг нашли, ставим флаг, ид и ссылку
                                        elem1.isFind = True
                                        elem1.idPerson = elem2.idPerson
                                        elem1.linkPerson = elem2.linkPerson

                    # =============================================================================================
                    # рассматриваем случай, когда человека упомянули по тексту как (Фамилия Имя)
                    for elem1 in elemNewsParsed.listMembers:
                        # нераспознанный с инициалами
                        if (elem1.isFind == False and len(elem1.surnameNorm) > 1 and len(elem1.nameNorm) > 1):
                            # нашли нераспознанного
                            for elem2 in elemNewsParsed.listMembers:
                                # ищем среди распознанных по полному фио
                                if (elem2.isFind == True):
                                    if (elem1.surnameNorm.lower() == elem2.surnameNorm.lower() and elem1.nameNorm.lower() == elem2.nameNorm.lower()):
                                        # если вдруг нашли, ставим флаг, ид и ссылку
                                        elem1.isFind = True
                                        elem1.idPerson = elem2.idPerson
                                        elem1.linkPerson = elem2.linkPerson


                # ================================================ЧАСТЬ 3===========================================================================
                # часть 3 ищем единственное совпадения по (Фамилия И.О.) среди всех сотрудников
                # Здесь мы после того как нашли людей по полному фио и нашли их упоминания по (Фамилия И.О.) и (Фамилия Имя)
                # используем первый цикл по всем и ищем там тех кто упомянут был только как (Фамилия И.О.) и такой человек нашелся один среди сотрудников

                # проверяем а есть ли вообще такие кого не нашли по (Фамилия И.О.)
                flagFind = True
                for elem in elemNewsParsed.listMembers:
                    if (elem.isFind == False):
                        flagFind = False

                if flagFind == False:
                    for member in elemNewsParsed.listMembers:
                        if member.isFind == False and len(member.surnameNorm) > 1 and len(member.nameNorm) > 0 and len(member.patronymicNorm) > 0:
                            index = binarySearchSurnameInListEmployee(member,listEmployee)
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
                                        i = i - 1
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
                                        i = i + 1
                                    else:
                                        flag = False
                                # =============================================================
                                # теперь просматриваем сотрудников по индексу И
                                # провереям совпадают ли первый буквы имен c первой буквой имени в member, если нет - индекс удаляем
                                i = 0
                                while i < len(listIndex):
                                    if(member.nameNorm.lower()[0] != listEmployee[listIndex[i]].name.lower()[0]):
                                        listIndex.pop(i)
                                        i = i - 1
                                    i = i + 1
                                # ====================================================================
                                # теперь смотрим остались ли вообще ещё кандидаты в listIndex
                                if len(listIndex) == 0:
                                    pass # ниче не делаем, а значит в цикле к след. member переходит
                                else:
                                    # если остались, то сравниваем первые буквы отчеств
                                    i = 0
                                    while i < len(listIndex):
                                        # если первые буквы отчеств не совпали индекс удаляем
                                        if (member.patronymicNorm.lower()[0] != listEmployee[listIndex[i]].patronymic.lower()[0]):
                                            listIndex.pop(i)
                                            i = i - 1
                                        i = i + 1
                                # ======================================================================
                                # опять смотрим остались ли вообще ещё кандидаты в listIndex
                                if len(listIndex) == 0:
                                    pass # если пусто не нашли кандидатов, ниче не делаем, а значит в цикле к след. member переходит
                                elif len(listIndex) == 1:
                                    # если нашли ровно ОДНО соответствие, то высокая вероятность, что это тот сотрудник который нам нужен
                                    # запоминаем его ид и ссылку
                                    # (но вдруг это случайный человек и мы ошиблись и тогда тут ссылку сотрудника присвоим левому человеку (такова погрешность))
                                    member.isFind = True # сответствие нашли по полному фио
                                    member.idPerson = listEmployee[listIndex[0]].idperson # запомнили id
                                    member.linkPerson = listEmployee[listIndex[0]].link_person  # запомнили ссылку сотрудника
                                elif len(listIndex) > 1:
                                    pass
                                    # тут ситуация когда остались (несколько людей) индексы тех, у кого совпадение по (Фамилия И.О.)
                                    # индексы лежат в listIndex
                                    # раз несколько кандидатов, то мы точно не знаем кто из них кто и лучше пусть (Фамилия И.О.) будет без ссылки
                                    # но по хорошему опять же тут нужно давать выбор, ЕСЛИ это обработка при регистрации новой новости

        return isGoodExecution, '', listNewsParsed

    except Exception as errMessage:
        isGoodExecution = False
        return isGoodExecution, errMessage, listNewsParsed

def replaceFioInNewsOnLinkEmployee(listNewsParsed, listNews):



    


def main():
    # Переменные
    apiNews = 'https://api.ciu.nstu.ru/v1.0/news/schoolkids/'  # апи новостей без года
    apiEmployee = "https://api.ciu.nstu.ru/v1.0/data/proj/teachers"  # апи сотрудников
    apiHeader = "Http-Api-Key"  # заголовок ключа запроса для апи сотрудников
    apiKey = "Y!@#13dft456DGWEv34g435f"  # ключ запроса для апи сотрудников
    linkPerson = "https://ciu.nstu.ru/kaf/persons/"  # ссылка на страницу сотрудника без id

    listNews = []  # сюда получаем новости с api
    listEmployee = []  # сюда получаем сотрудников с api
    listNewsParsed = [] # тут храним найденных людей в новостях
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
                    print('Загружаем необработанные новости за ' + str(year) + 'год ...')
                    isGoodExecution, errMessage, listNews = getNewsFromDbExceptParsed(year)
                    if isGoodExecution == False: raise ValueError(errMessage)
                    print('Распознаем в новостях людей...')
                    isGoodExecution, errMessage, listNewsParsed = findFioInNewsByNatasha(listNews)
                    if isGoodExecution == False: raise ValueError(errMessage)
                    print('Ищем распознанных людей среди сотрудников...')
                    isGoodExecution, errMessage, listNewsParsed = findPersonInlistEmployeeOnSurname(listNewsParsed, listEmployee)
                    if isGoodExecution == False: raise ValueError(errMessage)
                    print('Заменяем в новостях ФИО сотрудников на ссылку его страницы...')
                    isGoodExecution, errMessage, listNews = replaceFioInNewsOnLinkEmployee(listNewsParsed, listNews)
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

