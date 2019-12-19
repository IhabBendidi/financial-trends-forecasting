from pymongo import MongoClient
from pymongo import MongoClient




client = MongoClient('mongodb://dbUser:abcd@cluster0-shard-00-00-7umqv.mongodb.net:27017,cluster0-shard-00-01-7umqv.mongodb.net:27017,cluster0-shard-00-02-7umqv.mongodb.net:27017/test?ssl=true&replicaSet=Cluster0-shard-0&authSource=admin&retryWrites=true&w=majority')
db = client.bmce
#bigram_collection = db.test
bigram_collection = db.tfidf
processed_collection = db.processed
reduced_collection = db.reduced
#reduced_collection = db.reducedtest
sentence_collection = db.sentences


def get_data_of_week(week,year,collection):
    results = collection.find({'year' : year,'week':week}).batch_size(200)
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





def compute_mean(period):
    raw_data = get_data_of_week(period[0],period[1],bigram_collection)
    median = 0
    record_count = 0
    for record in raw_data:
        bigram = record.get('bigram')
        bigram_records = bigram_collection.find({'bigram':bigram,'week':period[0],'year':period[1]})
        summ = 0
        count = 0
        for bigram_record in bigram_records :
            summ += bigram_record.get('TF-IDF')
            count += 1
        mean = summ/count
        record_count +=1
        record['mean'] = mean
        median += mean
        bigram_collection.save(record)
    mean_mean = median / record_count
    return mean_mean


def reduce_bigrams(period,median):
    raw_data = get_data_of_week(period[0],period[1],bigram_collection)
    for record in raw_data:
        if record.get('mean') >= median:
            reduced_collection.save(record)

def recompute_mean(period):
    raw_data = get_data_of_week(period[0],period[1],reduced_collection)
    record_count = 0
    median = 0
    for record in raw_data:
        bigram = record.get('bigram')
        bigram_records = reduced_collection.find({'bigram':bigram,'week':period[0],'year':period[1]})
        summ = 0
        count = 0
        urls = []
        ids = []
        #url = record['article'].get('article_urls')
        #urls.append(url)
        for bigram_record in bigram_records :
            summ += bigram_record.get('TF-IDF')
            articles_urls = bigram_record['article'].get('article_urls')
            article_ids = bigram_record['article'].get('_id')
            urls.append(articles_urls)
            ids.append(article_ids)
            if count > 0 :
                reduced_collection.remove({'_id':bigram_record.get('_id'),'bigram':bigram_record.get('bigram'),'week':period[0],'year':period[1]})
            count += 1
        mean = summ/count
        record_count +=1
        record['urls'] = urls
        record['article_ids'] = ids
        record['mean'] = mean
        median += mean
        reduced_collection.save(record)
    mean_mean = median / record_count
    return mean_mean

def get_score_sentences(period,collection,median):
    raw_data = get_data_of_week(period[0],period[1],reduced_collection)
    for record in raw_data:
        bigram = record.get('bigram')
        sentiment_cursor = collection.find({'bigram':bigram,'week':period[0],'year':period[1]})
        opinion_score,sentences = count_mean_sentiment(sentiment_cursor)
        record['median'] = median
        record['sentences'] = sentences
        record['anomaly_point'] = record.get('mean') - median
        record['opinion_score'] = opinion_score
        reduced_collection.save(record)

def count_mean_sentiment(cursor):
    mean = 0
    count = 0
    sentences = []
    for record in cursor :
        sentiment = record.get('sentiment')
        sentences.append([record.get('sentence'),sentiment])
        count += 1
        mean += sentiment
    score = mean/count
    return score,sentences



def do(period):
    client = MongoClient('mongodb://dbUser:abcd@cluster0-shard-00-00-7umqv.mongodb.net:27017,cluster0-shard-00-01-7umqv.mongodb.net:27017,cluster0-shard-00-02-7umqv.mongodb.net:27017/test?ssl=true&replicaSet=Cluster0-shard-0&authSource=admin&retryWrites=true&w=majority')
    db = client.bmce
    #bigram_collection = db.test
    bigram_collection = db.tfidf
    processed_collection = db.processed
    reduced_collection = db.reduced
    #reduced_collection = db.reducedtest
    sentence_collection = db.sentences
    median = compute_mean(period)
    reduce_bigrams(period,median)
    median2 = recompute_mean(period)
    get_score_sentences(period,sentence_collection,median2)


if __name__ == "__main__":
    #periods = get_weeks(bigram_collection)

    p1 = multiprocessing.Process(target=do,args=([35,2019],))
    p1.start()
    p2 = multiprocessing.Process(target=do,args=([34,2019],))
    p2.start()
    p3 = multiprocessing.Process(target=do,args=([33,2019],))
    p3.start()
    p4 = multiprocessing.Process(target=do,args=([32,2019],))
    p4.start()
    p5 = multiprocessing.Process(target=do,args=([31,2019],))
    p5.start()
    p6 = multiprocessing.Process(target=do,args=([30,2019],))
    p6.start()
    do([36,2019])

    #print(periods)
    #for i in range(0,len(periods),2):
        #p1 = multiprocessing.Process(target=do,args=(periods[i+1],))
        #p1.start()
        #do(periods[i])
