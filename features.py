from pymongo import MongoClient
from pymongo import MongoClient


client = MongoClient('mongodb://dbUser:abcd@cluster0-shard-00-00-7umqv.mongodb.net:27017,cluster0-shard-00-01-7umqv.mongodb.net:27017,cluster0-shard-00-02-7umqv.mongodb.net:27017/test?ssl=true&replicaSet=Cluster0-shard-0&authSource=admin&retryWrites=true&w=majority')
db = client.bmce
reduced_collection = db.reduced
minimum = 27




def get_data_of_week(week,year,collection):
    results = collection.find({'year' : year,'week':week}).batch_size(200)
    return results

def get_persistence_weeks(week):
    weeks = []
    for i in range(week-4,week+1):
        weeks.append(i)
    return weeks

def get_persistence_score(bigram,week,collection):
    weeks = get_persistence_weeks(week)
    somme = 0
    count = len(weeks)
    for w in weeks :
        raw_bigram = collection.find_one({'bigram':bigram,'year':2019,'week':w})
        if raw_bigram == None :
            loi = 0
        elif w < minimum:
            loi = 0
        else :
            mean = raw_bigram.get('mean')
            median = raw_bigram.get('median')
            if mean<median :
                loi = 0
            else :
                loi = 1
        somme += loi
    score = somme / count
    return score



def get_weekly_persistence_score(week,collection):
    raw_data = collection.find({'week':week,'year':2019}).batch_size(200)
    for record in raw_data:
        bigram = record.get('bigram')
        persistence_score = get_persistence_score(bigram,week,collection)
        record['persistence_score'] = persistence_score
        collection.save(record)



def get_anomaly_score(bigram,week,collection):
    #weeks = [week-1,week]
    anomaly_score = 0
    if week == minimum :
        anomaly_score = 0
    else :
        record1 = collection.find_one({'bigram':bigram,'year':2019,'week':week})
        record2 = collection.find_one({'bigram':bigram,'year':2019,'week':week-1})
        if record2 == None :
            anomaly_point_1 = record1.get('anomaly_point')
            anomaly_point_2 = 0
            anomaly_score = anomaly_point_1 - anomaly_point_2
        else :
            anomaly_point_1 = record1.get('anomaly_point')
            anomaly_point_2 = record2.get('anomaly_point')
            anomaly_score = anomaly_point_1 - anomaly_point_2
    return anomaly_score


def get_weekly_anomaly_score(week,collection):
    raw_data = collection.find({'week':week,'year':2019}).batch_size(200)
    for record in raw_data:
        bigram = record.get('bigram')
        anomaly_score = get_anomaly_score(bigram,week,collection)
        record['anomaly_score'] = anomaly_score
        collection.save(record)



if __name__ == "__main__":
    for i in range (27,47):
        get_weekly_anomaly_score(i,reduced_collection)
        get_weekly_persistence_score(i,reduced_collection)
