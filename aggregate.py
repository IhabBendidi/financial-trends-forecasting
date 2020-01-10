from pymongo import MongoClient
from pymongo import MongoClient

client = MongoClient('localhost',27017)
db = client.bmce
bmce = db.b
agro = db.agro




def get_data_of_week(week,year,collection):
    results = collection.find({'year' : year,'week':week}).batch_size(1000)
    return results



def _check_exist(week,year,calendar):
    exists = False
    for value in calendar :
        if week == value[0] and year == value[1]:
            exists = True
            break
    return exists

def get_weeks(collection):
    cursor = collection.find()
    results = []
    for record in cursor :
        week = record.get('week')
        year = record.get('year')
        if len(results) == 0 and year == 2019:
            results.append([week, year])
        elif year == 2019 :
            exists = _check_exist(week,year,results)
            if exists==False :
                results.append([week, year])
    return results




def transform_week_data(week,year,collection):
    results = []
    raw_data = get_data_of_week(week,year,bmce)
    for record in raw_data :
        results.append(record)
        print(record)
    output = {'bigramsList':results,'week':week,'year':year}
    agro.insert(output)




if __name__ == "__main__":
    periods = get_weeks(bmce)
    for period in periods :
        transform_week_data(period[0],period[1],agro)
