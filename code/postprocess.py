from pymongo import MongoClient

# generate random integer values
from random import seed
from random import randint
client = MongoClient('mongodb://dbUser:abcd@cluster0-shard-00-00-7umqv.mongodb.net:27017,cluster0-shard-00-01-7umqv.mongodb.net:27017,cluster0-shard-00-02-7umqv.mongodb.net:27017/test?ssl=true&replicaSet=Cluster0-shard-0&authSource=admin&retryWrites=true&w=majority')
db = client.bmce
reduced_collection = db.reduced
bmce_collection = db.bmce2
seed(1)


def get_data_of_week(week,year,collection):
    results = collection.find({'year' : year,'week':week}).sort( { amount: -1 } )
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



def format_new_data(period):
    raw_data = raw_data = reduced_collection.find({'year' : period[1],'week':period[0]}).sort({ 'persistence_score': -1 })
    #get_data_of_week(period[0],period[1],reduced_collection)
    anomaly_max = reduced_collection.find().sort({'anomaly_score':-1}).limit(1)
    anomaly_min = reduced_collection.find().sort({'anomaly_score':1}).limit(1)
    for i in range(0,100):
        persistence = raw_data[i].get('persistence_score')
        PersistanceScopre = int(persistence *100) #############
        anomaly = raw_data[i].get('anomaly_score')
        updown = ""#####
        if anomaly < 0:
            updown = "neg"
        else :
            updown = "pos"
        initial_norm = (anomaly - anomaly_min)/(anomaly_max - anomaly_min)
        AnomalieScore = int(initial_norm * 100) ################

        #entreprises = ["O.n.d.a.", "Résidences Dar Saada", "Holcim Maroc"]#############
        Topic_Bigram = "Crise Immoubilière"################
        the_class =  "topicsElement"###############""
        finalScore = 40#####################
        top = 'true'########################""
        names_prediction = [week_to_date(period[0]+1),week_to_date(period[0]+2)]

        value1 = randint(10, 70)
        value2 = randint(10, 70)
        prediction = {'names':names_prediction,'values':[value1,value2]}################
        articles = []##############################
        urls = raw_data[i].get('urls')
        for k in range(0,len(urls),2) :
            articles.append({'articles_url':urls[k],'article_title':urls[k],'article_date':week_to_date(period[0]})
        opinion = raw_data[i].get('opinion_score')
        global_opinion_score = opinion * 100 #########################
        sentences = raw_data[i].get('sentences')
        comments = []######################
        for sentence in sentences :
            comments.append({'opinion_score':sentence[1]*100,'Phrase':sentence[0],'Date_Article':week_to_date(period[0]})
        bigram = raw_data[i].get('bigram')
        dates = get_anomaly_previous_weeks(period,bigram,anomaly_max,anomaly_min)####################""
        obj = {'PersistanceScopre':PersistanceScopre,'AnomalieScore':AnomalieScore,'Topic_Bigram':Topic_Bigram,'class':the_class,'updown':updown,'finalScore':finalScore,'top':top,'prediction':prediction,'articles':articles,'global_opinion_score':global_opinion_score,'comments':comments,'dates':dates}
        bmce_collection.save(obj)





def get_anomaly_previous_weeks(period,bigram,max,min):
    names = []
    values = []
    for j in range(1,6):
        week_index = 6 - j
        record = reduced_collection.find_one({'year' : period[1],'week':period[0],'bigram':bigram})
        anomaly = record.get('anomaly_score')
        initial_norm = (anomaly - min)/(max - min)
        score = int(initial_norm * 100)
        names.append(week_to_date(period[0])
        values.append(score)
    dates = {'names':names,'values':values}
    return dates



def week_to_date(week):
    month = week // 4
    day = (week % 4) + 1
    year = 2019
    date = str(day)+"/"+str(month)+'/'+str(year)
    return date





if __name__ == "__main__":
    periods = get_weeks(reduced_collection)
    for period in periods :
        format_new_data(period)
