import requests
import json
import csv
from time import sleep

class ParserEgzRef() :
    URL_BASE = 'https://ows.goszakup.gov.kz'
    SUFFIX_SUBJECT_ALL = '/v2/subject/all'
    SUFFIX_SUBJECT = '/v2/subject'
    SUFFIX_COUNTRY = '/v2/refs/ref_countries'
    
    with open('key.txt', 'r') as file: 
        API_KEY = file.read().replace('\n', '')    
    next_page_url = ''
    
    def makeUrlSubject(self):
        new_url = ''
        if (self.next_page_url == ''):
            new_url = self.URL_BASE + self.SUFFIX_SUBJECT
        else:
            new_url = self.URL_BASE + self.next_page_url
        return new_url
    
    def makeUrlSubjectAll(self):
        new_url = ''
        if (self.next_page_url == ''):
            new_url = self.URL_BASE + self.SUFFIX_SUBJECT_ALL
        else:
            new_url = self.URL_BASE + self.next_page_url
        return new_url
    
    def makeUrlCountry(self):
        new_url = ''
        if (self.next_page_url == ''):
            new_url = self.URL_BASE + self.SUFFIX_COUNTRY
        else:
            new_url = self.URL_BASE + self.next_page_url
        return new_url
    
    def makeRequest(self, url):
        headers = {'Content-Type': 'application/json; charset=utf-8', 'Authorization': 'Bearer ' + API_KEY}
        response = requests.get(url, headers=headers, verify=False)
        return response

    def parseSubjects(self):
        self.write_to_file_headers_subject()
        i = 1
        while (True):
            data = []
            url = self.makeUrlSubject()
            print(url)
            response = self.makeRequest(url).json()
            self.next_page_url = response["next_page"]
            for it in response['items']:
                item = {}
                item['number'] = i
                item['pid'] = it['pid']
                item['bin'] = it['bin']
                item['iin'] = it['iin']
                item['name_ru'] = it['name_ru']
                item['name_kz'] = it['name_kz']
                data.append(item)
                i += 1
            self.write_to_file_subject(data)
            if self.next_page_url == (None or ""):
                break
            sleep(0.1)

    def parseSubjectsAll(self):
        self.write_to_file_headers_subject_all()
        i = 1
        while (True):
            data = []
            url = self.makeUrlSubjectAll()
            print(url)
            response = self.makeRequest(url).json()
            self.next_page_url = response["next_page"]
            for it in response['items']:
                item = {}
                item['number'] = i
                item['pid'] = it['pid']
                item['bin'] = it['bin']
                item['iin'] = it['iin']
                item['name_ru'] = it['name_ru']
                item['name_kz'] = it['name_kz']
                item['unp'] = it['unp']
                item['regdate'] = it['regdate']
                item['crdate'] = it['crdate']
                item['number_reg'] = it['number_reg']
                item['series'] = it['series']
                item['full_name_ru'] = it['full_name_ru']
                item['full_name_kz'] = it['full_name_kz']
                item['email'] = it['email']
                item['phone'] = it['phone']
                item['website'] = it['website']
                item['last_update_date'] = it['last_update_date']
                item['country_code'] = it['country_code']
                item["qvazi"] = it['qvazi']
                item["customer"] = it["customer"]
                item["organizer"] = it["organizer"]
                item["mark_national_company"] = it["mark_national_company"]
                item["ref_kopf_code"] = it["ref_kopf_code"]
                item["mark_assoc_with_disab"] = it["mark_assoc_with_disab"]
                item["year"] = it["year"]
                item["mark_resident"] = it["mark_resident"]
                item["supplier"] = it["supplier"]
                item["krp_code"] = it["krp_code"]
                item["oked_list"] = it["oked_list"]
                item["kse_code"] = it["kse_code"]
                item["mark_world_company"] = it["mark_world_company"]
                item["mark_state_monopoly"] = it["mark_state_monopoly"] 
                item["mark_natural_monopoly"] = it["mark_natural_monopoly"]
                item["mark_patronymic_producer"] = it["mark_patronymic_producer"]
                item["mark_patronymic_supplyer"] = it["mark_patronymic_supplyer"]
                item["mark_small_employer"] = it["mark_small_employer"]
                item["type_supplier"] = it["type_supplier"]
                item["is_single_org"] = it["is_single_org"]
                item["system_id"] = it["system_id"]
                item["index_date"] = it["index_date"]
                
                data.append(item)
                i += 1
            self.write_to_file_subject_all(data)
            if self.next_page_url == (None or ""):
                break
            sleep(0.1)

    def parseCountries(self):
        self.write_to_file_headers_country()
        i = 1
        while (True):
            data = []
            url = self.makeUrlCountry()
            print(url)
            response = self.makeRequest(url).json()
            self.next_page_url = response["next_page"]
            for it in response['items']:
                item = {}
                item['number'] = i
                item['code'] = it['code']
                item['name_ru'] = it['name_ru']
                item['name_kz'] = it['name_kz']
                item['code_2'] = it['code_2']
                item['code_3'] = it['code_3']
                data.append(item)
                i += 1
            self.write_to_file_country(data)
            if self.next_page_url == (None or ""):
                break
            sleep(0.1)
                
    def write_to_file_headers_subject(self):
        with open("subjects.csv", "a", newline="", encoding='utf-8') as file:
            writer = csv.DictWriter(file, ("#","id организации",  "БИН", "ИИН",'Наименование на рус.',"Наименование на каз."))
            writer.writeheader()
        file.close()
                
    def write_to_file_headers_subject_all(self):
        with open("subjects_all.csv", "a", newline="", encoding='utf-8') as file:
            writer = csv.DictWriter(file, ("#","id организации",  "БИН", "ИИН",'Наименование на рус.',"Наименование на каз.", 
                                           "УНП", "Дата свидетельства о государственной регистрации",'Дата регистрации' "Номер свидетельства о государственной регистрации",
                                           "Серия свидетельства (для ИП)", "Полное наименование на русском языке", "Полное наименование на казахском языке", 
                                           "E-Mail", "Телефон", "Web сайт", "Дата последнего редактирования", "Страна по ЭЦП", "Флаг Квазисектора", 
                                           "Флаг Заказчик", "Флаг Организатор", "Флаг Национальная компания", "Код КОПФ", "Флаг Объединение инвалидов", 
                                           "Год регистрации", "Флаг резидента", "Флаг Поставщик", "Размерность предприятия (КРП)", "ОКЭД", "Код сектора экономики", 
                                           "Флаг Международная организация","Флаг Субъект государственной монополии", "Флаг Субъект естественной монополии", 
                                           "Флаг Отечественный товаропроизводитель","Флаг Отечественный поставщик",
                                           "Флаг Субъект малого предпринимательства (СМП)","Тип поставщика", 
                                           "Флаг Единый организатор","ИД Системы","Дата индексации"))
            writer.writeheader()
        file.close()
                
    def write_to_file_headers_country(self):
        with open("countries.csv", "a", newline="", encoding='utf-8') as file:
            writer = csv.DictWriter(file, ("#", "Код страны",  'Наименование на рус.', "Наименование на каз.", "Код 2", "Код 3"))
            writer.writeheader()
        file.close()
        
    def write_to_file_subject(self, data):
        with open("subjects.csv", "a", newline="", encoding='utf-8') as file:
            writer = csv.DictWriter(file, ("number", "pid", "bin","iin",'name_ru',"name_kz"))
            writer.writerows(data)
        file.close()
        
    def write_to_file_subject_all(self, data):
        with open("subjects_all.csv", "a", newline="", encoding='utf-8') as file:
            writer = csv.DictWriter(file, ("number", "pid", "bin","iin",'name_ru',"name_kz", 'unp', 'regdate', 
                                           'crdate', 'number_reg', 'series', 'full_name_ru', 'full_name_kz', 
                                           'email', 'phone', 'website', 'last_update_date', 'country_code', 'qvazi', 
                                           "customer", "organizer", "mark_national_company", "ref_kopf_code","mark_assoc_with_disab", 
                                           "year", "mark_resident", "supplier", "krp_code","oked_list","kse_code","mark_world_company",
                                           "mark_state_monopoly","mark_natural_monopoly","mark_patronymic_producer","mark_patronymic_supplyer",
                                           "mark_small_employer","type_supplier","is_single_org","system_id","index_date"))
            writer.writerows(data)
        file.close()
        
    def write_to_file_country(self, data):
        with open("countries.csv", "a", newline="", encoding='utf-8') as file:
            writer = csv.DictWriter(file, ("number", "code",'name_ru',"name_kz", "code_2","code_3"))
            writer.writerows(data)
        file.close()
        
def getSubjectsFromFile():
    subject_file = open("subjects_24.08.2021.csv", "r", encoding='utf-8')
    csv_reader = csv.reader(subject_file)
    next(csv_reader, None)
    subjects = []
    #dict_reader = csv.DictReader(subject_file)
    #subjects = list(dict_reader)
    for row in csv_reader:
        subject = {}
        subject['number'] = row[0]
        subject['pid'] = row[1]
        subject['bin'] = row[2]
        subject['iin'] = row[3]
        subject['name_ru'] = row[4]
        subject['name_kz'] = row[5]
        subject['index_date'] = row[6]
        subject['system_id'] = row[7]
        subject['index_date'] = row[8]
        subjects.append(subject)
    return subjects

def getSubjectsAllFromFile():
    subject_file = open("subjects_all_27.08.2021.csv", "r", encoding='utf-8')
    csv_reader = csv.reader(subject_file)
    next(csv_reader, None)
    subjects = []
    #dict_reader = csv.DictReader(subject_file)
    #subjects = list(dict_reader)
    for it in csv_reader:
        item = {}
        it.pop(0)
        item['pid'] = it[0]
        item['bin'] = it[1]
        item['iin'] = it[2]
        item['name_ru'] = it[3]
        item['name_kz'] = it[4]
        item['unp'] = it[5]
        item['regdate'] = it[6]
        item['crdate'] = it[7]
        item['number_reg'] = it[8]
        item['series'] = it[9]
        item['full_name_ru'] = it[10]
        item['full_name_kz'] = it[11]
        item['email'] = it[12]
        item['phone'] = it[13]
        item['website'] = it[14]
        item['last_update_date'] = it[15]
        item['country_code'] = it[16]
        item["qvazi"] = it[17]
        item["customer"] = it[18]
        item["organizer"] = it[19]
        item["mark_national_company"] = it[20]
        item["ref_kopf_code"] = it[21]
        item["mark_assoc_with_disab"] = it[22]
        item["year"] = it[23]
        item["mark_resident"] = it[24]
        item["supplier"] = it[25]
        item["krp_code"] = it[26]
        item["oked_list"] = it[27]
        item["kse_code"] = it[28]
        item["mark_world_company"] = it[29]
        item["mark_state_monopoly"] = it[30] 
        item["mark_natural_monopoly"] = it[31]
        item["mark_patronymic_producer"] = it[32]
        item["mark_patronymic_supplyer"] = it[33]
        item["mark_small_employer"] = it[34]
        item["type_supplier"] = it[35]
        item["is_single_org"] = it[36]
        item["system_id"] = it[37]
        item["index_date"] = it[38]
        subjects.append(item)
    return subjects
        
parser = ParserEgzRef()
parser.parseSubjects()
parser.parseCountries()
parser.parseSubjectsAll()