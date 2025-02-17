import requests
from datetime import datetime
from dataclasses import dataclass, field
import psycopg2
import time
import traceback

from natasha import Segmenter
from natasha import MorphVocab
from natasha import NewsEmbedding
from natasha import NewsMorphTagger
from natasha import NewsSyntaxParser
from natasha import NewsNERTagger
from natasha import NamesExtractor
from natasha import Doc

# ============================
# Структуры элементов списков
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
# =============================

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
        return isGoodExecution, 'Ошибка соединения с БД ' + database, conn

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
                    text_orig = ''
                    if jsonElem['TEXT'] == None: text_orig = ''
                    else: text_orig = jsonElem['TEXT']

                    listElem = News(jsonElem['ID'], jsonElem['URL'], jsonElem['TITLE'], text_orig,
                                    jsonElem['SHORTTEXT'], jsonElem['NEWS_DATE'], '', False, False, '')

                    listNews.append(listElem)

                return isGoodExecution, '', listNews

            else:
                raise ValueError('За ' + year + ' год новостей нет!')
        else:
            raise ValueError('Ошибка: api новостей вернуло код: ' + response.status_code)
    except Exception as errMessage:
        isGoodExecution = False
        lineNum = str(traceback.format_exc()).split(",")[1]
        return isGoodExecution,  str(lineNum) + ': '+ errMessage, listNews

# загрузка полученных новостей из listNews в БД
def loadNewsToDatabase(listNews):
    isGoodExecution = True
    conn = None
    try:
        countNumberNews = 0 # счетскик новостей
        pgDatabase, pgUser, pgPassword, pgHost, pgPort = getConnectionParametrs()
        isGoodExecution, message, conn = getConnection(pgDatabase, pgUser, pgPassword, pgHost, pgPort)
        if isGoodExecution == False:
            raise ValueError('Не удалось подключиться к БД: ' + message)
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
                el.id, el.url, el.title_orig, el.text_orig, el.shorttext, el.news_date, el.text_parse, el.is_parse, el.is_fio,
                el.url, el.title_orig, el.text_orig, el.shorttext, el.news_date, str(datetime.now()))
                cur.execute(query, data)
                countNumberNews = countNumberNews + 1

            if (conn != None):
                conn.commit()
                conn.close()
            return isGoodExecution, '', countNumberNews

    except psycopg2.Error as errMessage:
        if (conn != None):
            conn.rollback()
            conn.close()
        isGoodExecution = False
        lineNum = str(traceback.format_exc()).split(",")[1]
        return isGoodExecution,  str(lineNum) + ': Ошибка при загрузке полученных новостей в БД: ' + str(errMessage.diag.message_primary) + '\n' + str(errMessage.diag.message_detail), countNumberNews

    except Exception as errMessage:
        if (conn != None):
            conn.rollback()
            conn.close()
        isGoodExecution = False
        lineNum = str(traceback.format_exc()).split(",")[1]
        return isGoodExecution,  str(lineNum) + ': Ошибка при загрузке полученных новостей в БД: ' + str(errMessage), countNumberNews

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
        lineNum = str(traceback.format_exc()).split(",")[1]
        return isGoodExecution,  str(lineNum) + ': ' + str(errMessage), listEmployee

# загрузка полученных сотрудников из listEmployee в БД
def loadEmployeesToDatabase(listEmployee):
    isGoodExecution = True
    conn = None
    try:
        countEmployee = 0 # счетчик сотрудников
        pgDatabase, pgUser, pgPassword, pgHost, pgPort = getConnectionParametrs()
        isGoodExecution, message, conn = getConnection(pgDatabase, pgUser, pgPassword, pgHost, pgPort)
        if isGoodExecution == False:
            raise ValueError('Не удалось подключиться к БД: ' + message)
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
                el.idperson, el.name, el.surname, el.patronymic, el.link_person, el.post, el.chair_id, el.chair_id2, el.id,
                el.name, el.surname, el.patronymic, el.link_person, el.post, el.chair_id, el.chair_id2, el.id,
                str(datetime.now()))
                cur.execute(query, data)
                countEmployee = countEmployee + 1

            if (conn != None):
                conn.commit()
                conn.close()
            return isGoodExecution, '', countEmployee

    except psycopg2.Error as errMessage:
        if (conn != None):
            conn.rollback()
            conn.close()
        isGoodExecution = False
        lineNum = str(traceback.format_exc()).split(",")[1]
        return isGoodExecution,  str(lineNum) + ': Ошибка при загрузке полученных новостей в БД: ' + str(errMessage.diag.message_primary) + '\n' + str(errMessage.diag.message_detail), countEmployee

    except Exception as errMessage:
        if (conn != None):
            conn.rollback()
            conn.close()
        isGoodExecution = False
        lineNum = str(traceback.format_exc()).split(",")[1]
        return isGoodExecution,  str(lineNum) + ': Ошибка при загрузке полученных новостей в БД: ' + str(errMessage), countEmployee

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
        lineNum = str(traceback.format_exc()).split(",")[1]
        return isGoodExecution,  str(lineNum) + ': Ошибка при загрузке всех сотрудников из БД для обработки новостей:\n' + str(errMessage.diag.message_primary) + '\n' + str(errMessage.diag.message_detail), listEmployee

    except Exception as errMessage:
        if (conn != None):
            conn.rollback()
            conn.close()
        isGoodExecution = False
        lineNum = str(traceback.format_exc()).split(",")[1]
        return isGoodExecution,  str(lineNum) + ': Ошибка при загрузке всех сотрудников из БД для обработки новостей:\n' + str(errMessage), listEmployee

# получение новостей из БД кроме уже обработанных
def getNewsFromDbExceptParsed(year, is_parse):
    isGoodExecution = True
    listNews = []
    conn = None
    try:

        pgDatabase, pgUser, pgPassword, pgHost, pgPort = getConnectionParametrs()
        isGoodExecution, message, conn = getConnection(pgDatabase, pgUser, pgPassword, pgHost, pgPort)
        if isGoodExecution == False:
            raise ValueError('Не удалось подключиться к БД при получении новостей: \n' + message)
        else:
            cur = conn.cursor()
            query = """
                    SELECT id, url, title_orig, text_orig, shorttext, news_date, text_parse, is_parse, is_fio, update_ts
                    FROM public.news
                    WHERE EXTRACT(YEAR FROM news_date)::text = %s
                    AND is_parse = %s::boolean
                    ORDER BY id
                    ;"""
            data = (year, is_parse, )
            cur.execute(query, data)
            responseList = cur.fetchall()
            for elem in responseList:
                listElem = News(int(elem[0]), elem[1], elem[2], elem[3], elem[4], str(elem[5]), elem[6], bool(elem[7]), bool(elem[8]), str(elem[9]))
                listNews.append(listElem)

            if (len(listNews) == 0):
                raise ValueError('Необработанных новостей за ' + year + ' год в БД не найдено!')

            if (conn != None and conn.closed == 0):
                conn.commit()
                conn.close()

            return isGoodExecution, '', listNews

    except psycopg2.Error as errMessage:
        if (conn != None and conn.closed == 0):
            conn.rollback()
            conn.close()
        isGoodExecution = False
        lineNum = str(traceback.format_exc()).split(",")[1]
        return isGoodExecution,  str(lineNum) + ': Ошибка при загрузке полученных новостей из БД для их обработки: ' + str(errMessage.diag.message_primary) + '\n' + str(errMessage.diag.message_detail), listNews

    except Exception as errMessage:
        if (conn != None and conn.closed == 0):
            conn.rollback()
            conn.close()
        isGoodExecution = False
        lineNum = str(traceback.format_exc()).split(",")[1]
        return isGoodExecution,  str(lineNum) + ': Ошибка при загрузке полученных новостей из БД для их обработки: ' + str(errMessage), listNews

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

            try:
                listNewsMember.clear()
                # обрабатываем текст
                doc = Doc(elemNews.text_orig) # создаем объект из текста новости
                doc.segment(segmenter) # если убрать, то tag_ner выдаст исключение (выдает исключение если длина <= 50)
                doc.tag_morph(morph_tagger) # если это не сделать, но нормализация не сработает
                # doc.parse_syntax(syntax_parser) # not use
                doc.tag_ner(ner_tagger) # разбивает документ на теги, чтобы появились spans в doc
            except Exception as err:
                # если возникли исключения ошибки при парсинге, просто пропускаем, появляются случайно
                continue

            # смотрим распознанные сущности
            for span in doc.spans:
                # если сущность это человек (PERson)
                if span.type != None and span.type == 'PER':
                    try:
                        span.normalize(morph_vocab) # нормализует фио в span.normal
                        span.extract_fact(names_extractor) # заполняет фио в span.fact по полям

                        # заполняем нормализованное фио, если чего-то нет, то пустая строка
                        name = ''
                        surname = ''
                        patronymic = ''
                        if span.fact != None:
                            if span.fact.as_dict != None:
                                name = span.fact.as_dict.get('first', '')
                                surname = span.fact.as_dict.get('last', '')
                                patronymic = span.fact.as_dict.get('middle', '')

                                # запомнили всех распознанных людей в список
                                elemNewsMember = NewsMember(0, '', span.start, span.stop, str(name), str(surname), str(patronymic), False)
                                listNewsMember.append(elemNewsMember)
                    except Exception as err:
                        # если возникли исключения ошибки при парсинге, просто пропускаем, появляются случайно
                        pass
            # если Наташа никого не нашла, то ставим флаг False
            if len(listNewsMember) > 0:
                elemNewsParsed = NewsParsed(listNewsMember.copy(), elemNews.id, True)
            else:
                elemNewsParsed = NewsParsed(listNewsMember.copy(), elemNews.id, False)
            listNewsParsed.append(elemNewsParsed)

        return isGoodExecution, '', listNewsParsed

    except Exception as errMessage:
        isGoodExecution = False
        lineNum = str(traceback.format_exc()).split(",")[1]
        return isGoodExecution,  str(lineNum) + ': Ошибка при распозновании фио в новостях:\n' + str(errMessage), listNewsParsed

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
                                # по полному фио нашелся один человек, велика вероятность, что этот сотрудник и указан в новости
                                member.isFind = True # сответствие нашли по полному фио
                                member.idPerson = listEmployee[listIndex[0]].idperson # запомнили id
                                member.linkPerson = listEmployee[listIndex[0]].link_person  # запомнили ссылку сотрудника
                            elif len(listIndex) > 1:
                                pass
                                # тут ситуация когда остались индексы тех, у кого полное совпадение по фио
                                # индексы лежат в listIndex
                                # что делать - не знаю, поэтому ничего не делаем
                                # но по хорошему нужно давать выбор, если это обработка при регистрации одной новой новости

                # ==============================================ЧАСТЬ 2=============================================================
                # часть 2 ищем совпадения по (Фамилия И.О.) и (Фамилия Имя) СРЕДИ распознанных по полному совпалению ФИО
                # когда уже пробежались по всем людям из новости, смотрим есть ли хотябы один найденный
                flagFind = False
                for elem in elemNewsParsed.listMembers:
                    if(flagFind == False and elem.isFind == True):
                        flagFind = True

                # если  есть хоть один найденный по полному фио, то ставим в новости что  isFio = True и всё  к след. новости
                if (flagFind == False):
                    elemNewsParsed.isFio = False
                else:
                    # =============================================================================================
                    # иначе ставим, что хоть кто-то да найден
                    elemNewsParsed.isFio = True
                    # теперь будем пытаться искать ненайденных по уже найденным
                    # =============================================================================================
                    # рассматриваем случай 1, когда человека упомянули по тексту как (Фамилия И.О.)
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
                    # рассматриваем случай 2, когда человека упомянули по тексту как (Фамилия Имя)
                    for elem1 in elemNewsParsed.listMembers:
                        # нераспознанный с инициалами
                        if (elem1.isFind == False and len(elem1.surnameNorm) > 1 and len(elem1.nameNorm) > 1 and len(elem1.patronymicNorm) == 0):
                            # нашли нераспознанного
                            for elem2 in elemNewsParsed.listMembers:
                                # ищем среди распознанных по полному фио
                                if (elem2.isFind == True):
                                    if (elem1.surnameNorm.lower() == elem2.surnameNorm.lower() and elem1.nameNorm.lower() == elem2.nameNorm.lower()):
                                        # если вдруг нашли, ставим флаг, ид и ссылку
                                        elem1.isFind = True
                                        elem1.idPerson = elem2.idPerson
                                        elem1.linkPerson = elem2.linkPerson

                    # =============================================================================================
                    # !!!!! НЕ работает это сопоставление, потому что natasha распознает (Имя Отчество) с ошибкой: отчество как фамилия распознается
                    # раскоментить два if и закоментить два других и будет работать
                    # рассматриваем случай 3, когда человека упомянули по тексту как (Имя Отчество)
                    for elem1 in elemNewsParsed.listMembers:
                        # нераспознанный с инициалами
                        if (elem1.isFind == False and len(elem1.nameNorm) > 1 and len(elem1.patronymicNorm) > 1):
                        # if (elem1.isFind == False and len(elem1.nameNorm) > 1 and len(elem1.surnameNorm) > 1 and len(elem1.patronymicNorm) == 0):
                            # нашли нераспознанного
                            for elem2 in elemNewsParsed.listMembers:
                                # ищем среди распознанных по полному фио
                                if (elem2.isFind == True):
                                    if (elem1.nameNorm.lower() == elem2.nameNorm.lower() and elem1.patronymicNorm.lower() == elem2.patronymicNorm.lower()):
                                    # if (elem1.nameNorm.lower() == elem2.nameNorm.lower() and elem1.surnameNorm.lower() == elem2.patronymicNorm.lower()):
                                        # если вдруг нашли, ставим флаг, ид и ссылку
                                        elem1.isFind = True
                                        elem1.idPerson = elem2.idPerson
                                        elem1.linkPerson = elem2.linkPerson
                    # =============================================================================================


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
                    # опять проверяем смогли ли мы найти кого-то
                    flagFind = False
                    for elem in elemNewsParsed.listMembers:
                        if (elem.isFind == True):
                            flagFind = True

                    # если  есть хоть один найденный по полному фио, то ставим в новости что  isFio = True и всё  к след. новости
                    if (flagFind == True):
                        elemNewsParsed.isFio = True




        return isGoodExecution, '', listNewsParsed

    except Exception as errMessage:
        isGoodExecution = False
        lineNum = str(traceback.format_exc()).split(",")[1]
        return isGoodExecution,  str(lineNum) + ': Ошибка при сопоставлении фио и сотрудников: \n' + str(errMessage), listNewsParsed

# двоичный поиск индекса новости в списке новостей по ID новости
def findIndexInListNewsOnIdNews(idNews, listNews):
    # Обычным двоичным поиском ищем индекс новости
    # возвращает индекс новости в списке listNews или -1
    start = 0
    end = len(listNews) - 1

    while start <= end:
        mid = (start + end) // 2
        if idNews < listNews[mid].id:
            end = mid - 1
        elif idNews > listNews[mid].id:
            start = mid + 1
        elif idNews == listNews[mid].id:
            return mid

    return -1

# удаление не распознанных людей из списка в ListNewsParsed
def deleteNotFindMembersFromlistMembersInListNewsParsed(listMembers):
    i = 0
    while i < len(listMembers):
        if (listMembers[i].isFind == False):
            listMembers.pop(i)
            i = i - 1
        i = i + 1
    return listMembers

# сортировка ListMembers по StartPosition
def sortListMembersOnStartPosition(listMembers):
    listMembers = sorted(listMembers, key=lambda x: x.startPos)
    return listMembers

# замена фио на ссылки найденных сотрудников
def replaceFioInNewsOnLinkEmployee(listNewsParsed, listNews):

    try:
        isGoodExecution = True
        linkPart1 = '<a href="'
        linkPart2 = '" target="_blank" rel="noopener">'
        linkPart3 = '</a>'
        # счетчики для статистики
        countIsParsedNews = 0 # сколько всего обраотали
        countIsFioNewsTrue = 0 # в скольких новостях найдены ФИО
        countIsFioNewsFalse = 0 # в скольких новостях не найдены ФИО


        for elemNewsParsed in listNewsParsed:
            index = findIndexInListNewsOnIdNews(elemNewsParsed.idNews, listNews) # индекс новости по id
            if (index < 0 or index >= len(listNews)): # если вдруг индекс не найден, то ничего не делаем
                pass
            else:
                # индекс новости нашли
                countIsParsedNews = countIsParsedNews + 1 # счетчик статистики
                listNews[index].is_parse = True # в любом случае ставим что мы обработали новость

                if elemNewsParsed.isFio == False:
                    # если в новости не найдены люди (или найдены, но не распознанны как сотрудники)
                    countIsFioNewsFalse = countIsFioNewsFalse + 1 #счетчик статистики
                    listNews[index].is_fio = False # ставим флаг
                    # listNews[index].text_parse = listNews[index].text_orig  # текст парсенный пусть пустой
                else:
                    # если все таки у нас найденный люди и нужна замена


                    elemNewsParsed.listMembers = deleteNotFindMembersFromlistMembersInListNewsParsed(elemNewsParsed.listMembers) # удалили нераспознанных members
                    elemNewsParsed.listMembers = sortListMembersOnStartPosition(elemNewsParsed.listMembers) # отсортировали ListMembers

                    # формируем новый текст новости
                    newText = listNews[index].text_orig # новый текст изначально равен оригинальному, в нем и делаем замену
                    diffSize = 0
                    n = len(elemNewsParsed.listMembers)
                    #флаг, что остались люди
                    if(n > 0):
                        listNews[index].is_fio = True  # ставим флаг
                        countIsFioNewsTrue = countIsFioNewsTrue + 1  # счетчик для статистики
                    else:
                        countIsFioNewsFalse = countIsFioNewsFalse + 1  # счетчик статистики

                    for i in range(0, n):
                        start = elemNewsParsed.listMembers[i].startPos # начало замены
                        end = elemNewsParsed.listMembers[i].stopPos # конец замены
                        fioText = newText[start:end]
                        # строим ссылку
                        linkText = linkPart1 + str(elemNewsParsed.listMembers[i].linkPerson) + linkPart2 + fioText + linkPart3
                        fioSize = len(fioText)
                        linkSize = len(linkText)
                        diffSize = linkSize - fioSize # разница

                        newText = (newText[0:start] + linkText + newText[start+fioSize:]) # сформировали новый текст

                        # сдвигаем индексы у следующих людей
                        for j in range(i+1, n):
                            elemNewsParsed.listMembers[j].startPos = elemNewsParsed.listMembers[j].startPos + diffSize
                            elemNewsParsed.listMembers[j].stopPos = elemNewsParsed.listMembers[j].stopPos + diffSize

                    # запомнили новый текст
                    listNews[index].text_parse = newText


        return isGoodExecution, '', listNews, countIsParsedNews, countIsFioNewsTrue, countIsFioNewsFalse

    except Exception as errMessage:
        isGoodExecution = False
        lineNum = str(traceback.format_exc()).split(",")[1]
        return isGoodExecution,  str(lineNum) + ': Ошибка при замене фио на ссылки в новостях: ' + str(errMessage), listNews, countIsParsedNews, countIsFioNewsTrue, countIsFioNewsFalse

# обновление обработанных новостей в БД
def updateNewsInDatabase(listNews):
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
                query = """ UPDATE public.news
                            SET
                            text_parse = %s, 
                            is_parse = %s::boolean, 
                            is_fio = %s::boolean, 
                            update_ts = %s::TIMESTAMP
                            WHERE
                            id = %s
                            ;"""
                data = (el.text_parse, el.is_parse, el.is_fio, str(datetime.now()), el.id, )
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
        lineNum = str(traceback.format_exc()).split(",")[1]
        return isGoodExecution,  str(lineNum) + ': Ошибка при обновлении обработанный новостей в БД: ' + str(errMessage.diag.message_primary) + '\n' + str(errMessage.diag.message_detail)

    except Exception as errMessage:
        if (conn != None):
            conn.rollback()
            conn.close()
        isGoodExecution = False
        lineNum = str(traceback.format_exc()).split(",")[1]
        return isGoodExecution,  str(lineNum) + ': Ошибка при обновлении обработанный новостей в БД: ' + str(errMessage)

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
    errMessage = 'Сообщение об ошибках'
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
                    print('Получение новостей с api...')
                    isGoodExecution, errMessage, listNews = getNewsFromApi(apiNews, year)
                    if isGoodExecution == False: raise ValueError(errMessage)
                    print('Загружаем новости в БД...')
                    isGoodExecution, errMessage, countNumberNews = loadNewsToDatabase(listNews)
                    if isGoodExecution == False: raise ValueError(errMessage)
                    print('Новости в количестве ' + str(countNumberNews) + ' загружены.')

            elif choice == 2:
                print('Получение сотрудников с api...')
                isGoodExecution, errMessage, listEmployee = getEmployeesFromApi(apiEmployee, apiHeader, apiKey, linkPerson)
                if isGoodExecution == False: raise ValueError(errMessage)
                print('Загружаем сотрудников в БД...')
                isGoodExecution, errMessage, countEmployee = loadEmployeesToDatabase(listEmployee)
                if isGoodExecution == False: raise ValueError(errMessage)
                print('Сотрудники в количестве ' + str(countEmployee) + ' загружены.')

            elif choice == 3:
                year = input('Введите год, за который обработать новости: ')
                if (checkYearNews(year)) == False:
                    raise ValueError('Год должен быть от 2007 по ' + str(datetime.now().year) + '!')
                else:
                    print('Загружаем всех сотрудников...')
                    isGoodExecution, errMessage, listEmployee = getEmployeesFromDb()
                    if isGoodExecution == False: raise ValueError(errMessage)
                    print('Загружаем необработанные новости за ' + str(year) + ' год ...')
                    isGoodExecution, errMessage, listNews = getNewsFromDbExceptParsed(year, False)
                    if isGoodExecution == False: raise ValueError(errMessage)
                    start_time = time.time()
                    print('Распознаем в новостях людей...')
                    isGoodExecution, errMessage, listNewsParsed = findFioInNewsByNatasha(listNews)
                    if isGoodExecution == False: raise ValueError(errMessage)
                    print('Ищем распознанных людей среди сотрудников...')
                    isGoodExecution, errMessage, listNewsParsed = findPersonInlistEmployeeOnSurname(listNewsParsed, listEmployee)
                    if isGoodExecution == False: raise ValueError(errMessage)
                    print('Заменяем в новостях ФИО сотрудников на ссылки...')
                    isGoodExecution, errMessage, listNews, countIsParsedNews, countIsFioNewsTrue, countIsFioNewsFalse = replaceFioInNewsOnLinkEmployee(listNewsParsed, listNews)
                    end_time = time.time()
                    if isGoodExecution == False: raise ValueError(errMessage)
                    countTimeParsed = (end_time - start_time)
                    print('Загружаем в БД обработанные новости...')
                    isGoodExecution, errMessage = updateNewsInDatabase(listNews)
                    if isGoodExecution == False: raise ValueError(errMessage)
                    print('Новости за ' + str(year) + ' год обработаны.')
                    # вывод статистики
                    print('Статистика работы:')
                    print('Время, затраченное на распознование и сопоставление: ' + str(countTimeParsed) + 'sec')
                    print('Количество обработанных новостей: ' + str(countIsParsedNews))
                    print('Количество новостей, в которых распознанны сотрудники: ' + str(countIsFioNewsTrue))
                    print('Количество новостей, в которых сотрудники НЕ найдены: ' + str(countIsFioNewsFalse))


            elif choice == 0:
                flagMenu = False
                raise ValueError('Пользователь завершил работу программы.')

            else:
                raise ValueError('Действие не найдено.')

        except Exception as Message:
            print(Message)


if __name__ == '__main__':
    main()
