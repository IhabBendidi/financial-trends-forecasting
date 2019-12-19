from pymongo import MongoClient
from pymongo import MongoClient
import re





article_dates = list()




#client = MongoClient("mongodb+srv://dbUser:dbUser@scrapping-oojgw.mongodb.net/test?retryWrites=true&w=majority")

#db=client.selenium
#collection = db.articles2



client = MongoClient('localhost', 27017)
db = client.bmce
collection = db.articles
processed_collection = db.processed
cursor = collection.find()

def month_to_num (month):
    if month == "janvier" or month == "Janvier" or month == "janvier".upper():
        num = '01'
    elif month == "février" or month == "Février" or month == "fevrier" or month == "Fevrier" or month == "février".upper() or month == "fevrier".upper():
        num = '02'
    elif month == "mars" or month == "Mars" or month == "mars".upper():
        num = '03'
    elif month == "avril" or month == "Avril" or month == "avril".upper():
        num = '04'
    elif month == "mai" or month == "Mai" or month == "mai".upper():
        num = '05'
    elif month == "juin" or month == "Juin" or month == "juin".upper():
        num = '06'
    elif month == "juillet" or month == "Juillet" or month == "juillet".upper():
        num = '07'
    elif month == "aout" or month == "Aout" or month == "août" or month == "Août" or month == "aout".upper() or month == "août".upper():
        num = '08'
    elif month == "septembre" or month == "Septembre" or month == "septembre".upper():
        num = '09'
    elif month == "octobre" or month == "Octobre" or month == "octobre".upper():
        num = '10'
    elif month == "novembre" or month == "Novembre" or month == "novembre".upper():
        num = '11'
    elif month == "décembre" or month == "Décembre" or month == "decembre" or month == "Decembre" or month == "décembre".upper() or month == "decembre".upper():
        num = '12'
    else :
        print("Month_to_num : Month format wrong")
        print(month)
        num = '00'
    return num

def standarize_date(date):
    components = date.split(" ")
    day = components[0]
    year = components[2]
    month = month_to_num(components[1])
    result = year + '-' + month + '-' + day
    return result




def save_date (date,record,collection,week,year):
    article_titles = record.get('article_title')
    article_authors = record.get('article_author')
    article_dates = date
    article_bodies = record.get('article_body')
    article_urls = record.get('articles_url')
    result = {'article_title' : article_titles ,'article_author':article_authors,'article_date':article_dates,'article_body': article_bodies,'article_urls': article_urls,'week': week,'year':year}
    collection.insert(result)


def date_to_week(date):
    year = int(date.split("-")[0])
    month = int(date.split("-")[1])
    day = int(date.split("-")[2])
    if month == 1 :
        week = int(day/7) + 1
    else :
        week = int((day + (month-1)*30)/7) + 1
    return week,year

def fix_date(cursor,collection):
    for record in cursor :
        raw_date = record.get('article_date')
        if len(re.findall("T[0123456789]", raw_date))>0 :
            temp_data = raw_date.split("T")[0].strip() # '2019-07-24'
            #article_dates.append(temp_data)
        elif len(raw_date.split("-")) == 3 :
            temp = raw_date.split(" - ")[0].strip() # '16 novembre 2019'
            temp_data = standarize_date(temp)
            #article_dates.append(temp_data)
        elif len(raw_date.split("-")) == 2 and len(raw_date.split("-")[0].strip().split(" ")) == 4 :
            t = raw_date.split("-")[0].strip()
            temp = t.split(" ")[1] + " " + t.split(" ")[2] + " " + t.split(" ")[3]
            temp_data = standarize_date(temp)
            #article_dates.append(temp_data)
        elif len(raw_date.split(" PAR ")) == 2 and len(raw_date.split(" PAR ")[0].strip().split(" ")) == 4 and raw_date.split(" PAR ")[1] == "LAQUOTIDIENNE":
            t = raw_date.split(" PAR ")[0].strip()
            temp = t.split(" ")[1] + " " + t.split(" ")[2] + " " + t.split(" ")[3]
            temp_data = standarize_date(temp)
            #article_dates.append(temp_data)
        elif len(raw_date.split(" | ")) == 3 :
            t = raw_date.split(" | ")[1].split("Le")[1].strip()
            temp_data = t.split("/")[2] + "-" + t.split("/")[1] + "-" + t.split("/")[0]
            #article_dates.append(temp_data)
        elif len(raw_date.split(" à ")) == 2 :
            temp = raw_date.split(" à ")[0].strip()
            temp_data = standarize_date(temp)
            #article_dates.append(temp_data)
        elif len(raw_date.split(" À ")) == 2 :
            t = raw_date.split(" À ")[0].strip()
            temp = t.split("-")[1].strip()
            temp_data = standarize_date(temp)
            #article_dates.append(temp_data)
        elif len(raw_date.split(" ")) == 3 :
            temp_data = standarize_date(raw_date)
            #article_dates.append(temp_data)
        week,year = date_to_week(temp_data)
        save_date(temp_data,record,collection,week,year)












if __name__ == "__main__":
    fix_date(cursor,processed_collection)
