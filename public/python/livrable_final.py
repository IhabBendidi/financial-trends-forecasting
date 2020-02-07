# coding=utf-8
from pymongo import MongoClient
import sys
import json
client = MongoClient('mongodb://dbUser:abcd@cluster0-shard-00-00-7umqv.mongodb.net:27017,cluster0-shard-00-01-7umqv.mongodb.net:27017,cluster0-shard-00-02-7umqv.mongodb.net:27017/test?ssl=true&replicaSet=Cluster0-shard-0&authSource=admin&retryWrites=true&w=majority')
#client = MongoClient('localhost', 27017)
db = client.bmce
bmce_collection = db.bmce2


entreprises={"names":["Office Cherifien des Phosphates", "Attijari Wafabank", "Adoha Group", "Maroc Telecom", "SNTL", "Legrand Maroc", "Alsa City", "Other"],"values":[10,20,5,15,33,17,5,5]}




def get_week_data(date):
    week = date_to_week(date)
    bigramsList = []
    raw_data = bmce_collection.find({'week':week})
    for record in raw_data:
        bigramsList.append(record)
    data = {'entreprises':entreprises,'bigramsList':bigramsList}
    with open('public/python/output.json', 'w') as outfile:
        json.dump(data, outfile)


def date_to_week(date):
    parts = date.split(' ')
    month = int(month_to_num(parts[1]))
    year = int(parts[3])

    day = int(parts[2])
    if month == 1 :
        week = int(day/7) + 1
    else :
        week = int((day + (month-1)*30)/7) + 1
    return week

def month_to_num (month):
    if month == "Jan" or month == "Janvier" or month == "janvier".upper():
        num = '01'
    elif month == "Feb" or month == "Février" or month == "fevrier" or month == "Fevrier" or month == "février".upper() or month == "fevrier".upper():
        num = '02'
    elif month == "Mar" or month == "Mars" or month == "mars".upper():
        num = '03'
    elif month == "Apr" or month == "Avril" or month == "avril".upper():
        num = '04'
    elif month == "May" or month == "Mai" or month == "mai".upper():
        num = '05'
    elif month == "Jun" or month == "Juin" or month == "juin".upper():
        num = '06'
    elif month == "Jul" or month == "Juillet" or month == "juillet".upper():
        num = '07'
    elif month == "Aug" or month == "Aout" or month == "août" or month == "Août" or month == "aout".upper() or month == "août".upper():
        num = '08'
    elif month == "Sep" or month == "Septembre" or month == "septembre".upper():
        num = '09'
    elif month == "Oct" or month == "Octobre" or month == "octobre".upper():
        num = '10'
    elif month == "Nov" or month == "Novembre" or month == "novembre".upper():
        num = '11'
    elif month == "Dec" or month == "Décembre" or month == "decembre" or month == "Decembre" or month == "décembre".upper() or month == "decembre".upper():
        num = '12'
    else :
        print("Month_to_num : Month format wrong")
        print(month)
        num = '00'
    return num



def get_all_data():
    dataset = []
    for i in range(27,47):
        raw_data = bmce_collection.find({'week':i})
        bigramsList = []
        for record in raw_data:
            record['_id']=0
            bigramsList.append(record)
        data = {'week':i,'entreprises':entreprises,'bigramsList':bigramsList}
        dataset.append(data)
    output = {'output':dataset}
    with open('output.json', 'w') as outfile:
        json.dump(output, outfile)






if __name__ == "__main__":
    date = sys.argv[1]
    #week,year = date_to_week(date)
    get_week_data(date)
    #get_all_data()



    #data = {'entreprises':entreprises,'bigramsList':bigramsList}
    #with open('public/python/output.json', 'w') as outfile:
    #    json.dump(data, outfile)
    #sys.stdout.write(str(week))
    #sys.stdout.write(str(year))
    #print({"entreprises":entreprises,"bigramsList":bigramsList})
