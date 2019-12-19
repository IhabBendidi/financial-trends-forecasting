import pandas as pd
import math
import numba
from numba import jit
import multiprocessing

import nltk
from nltk.tokenize import sent_tokenize, word_tokenize
from nltk.corpus import stopwords
from nltk.tokenize import RegexpTokenizer
from nltk import bigrams
from sklearn.feature_extraction.text import TfidfVectorizer

from pymongo import MongoClient
from pymongo import MongoClient



client = MongoClient('localhost', 27017)
db = client.bmce
processed_collection = db.processed
test_collection = db.tfidf
sentence_collection = db.sentences

index = 1

def get_data_of_week(week,year,collection):
    results = collection.find({'year' : year,'week':week}).batch_size(5000)
    return results


def _check_exist(week,year,calendar):
    exists = False
    for value in calendar :
        if week == value[0] and year == value[1]:
            exists = True
            break
    return exists

def get_weeks():
    cursor = processed_collection.find()
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


def occurence_count(bigram,bigrams):
    i = 0
    for bi in bigrams :
        if bigram == bi :
            i += 1
    return i

def compute_tf(bigram_with_occurence,bigrams):
    occurence_count = bigram_with_occurence[1]
    bigram_count = len(bigrams)
    tf = occurence_count / bigram_count
    return tf

@jit(nopython=True)
def logarithm(product):
    return math.log(product)


def division(a,b):
    return a/b


def compute_idf(bigram,period):
    #count_articles_per_period = processed_collection.count({'week':period[0],'year':period[1]})
    #print(count_articles_per_period)
    num_docs_of_bigram = len(test_collection.find({'bigram':bigram,'week':period[0],'year':period[1]}).distinct('article'))
    #print("\n\n")
    print(num_docs_of_bigram)
    #product = division(count_articles_per_period,num_docs_of_bigram)
    #idf = logarithm(product)
    return num_docs_of_bigram



def filter_unique_bigrams(bigrams):
    unique_bigrams = []
    unique_bigrams_with_count = []
    unique_bigrams.append(bigrams[0])
    for b in bigrams :
        if b not in unique_bigrams:
            unique_bigrams.append(b)
    for bi in unique_bigrams:
        unique_bigrams_with_count.append([bi,occurence_count(bi,bigrams)])
    return unique_bigrams_with_count


def extract_sentence_bigrams(article_body,record,collection):
    tokenizer = RegexpTokenizer(r'\w+')
    sentences = article_body.split(".")
    stopWords = set(stopwords.words('french'))
    full_words = []
    for sentence in sentences :
        words = tokenizer.tokenize(sentence)
        wordsFiltered = []
        for w in words:
            if w.lower() not in stopWords:
                if not w.isdigit():
                    wordsFiltered.append(w.lower())
        modified_sentence_body = ' '.join([w for w in wordsFiltered])
        wordsFiltered_bigrams_tmp=bigrams(modified_sentence_body.split())
        wordsFiltered_bigrams_sentence = []
        for a in wordsFiltered_bigrams_tmp:
            wordsFiltered_bigrams_sentence.append(a[0]+" "+a[1])
        for c in wordsFiltered_bigrams_sentence :
            #full_words.append([sentence,c])
            obj = {'article_title':record.get('article_title'),'article_urls':record.get('article_urls')}
            output = {'bigram':c,'sentence':sentence,'article':obj,'week':record.get('week'),'year':record.get('year')}
            collection.insert(output)
            full_words.append(c)
    return full_words


def count_bigram_total_articles(raw_data,bigr):
    i = 0
    j = 0
    for record in raw_data:
        j += 1
        article_body = record.get('article_body')
        stopWords = set(stopwords.words('french'))
        tokenizer = RegexpTokenizer(r'\w+')
        words = tokenizer.tokenize(article_body)
        wordsFiltered = []
        for w in words:
            if w.lower() not in stopWords:
                if not w.isdigit():
                    wordsFiltered.append(w.lower())
        modified_body = ' '.join([w for w in wordsFiltered])
        wordsFiltered_bigrams_tmp=bigrams(modified_body.split())
        wordsFiltered_bigrams = []
        for a in wordsFiltered_bigrams_tmp:
            wordsFiltered_bigrams.append(a[0]+" "+a[1])
        if bigr in wordsFiltered_bigrams:
            i += 1
        #raw_data.close()
    return i,j



def compute(periods):
    # Loops over all periods, create bigrams for each period and computes its tfIDF
    for period in periods :
        create_bigram(period)
        create_tfidf(period)

def compute_period(period):
    client = MongoClient('localhost', 27017)
    db = client.bmce
    processed_collection = db.processed
    test_collection = db.tfidf
    create_bigram(period,db)
    #create_tfidf(period,db)


def create_bigram(period,db):
    raw_data = get_data_of_week(period[0],period[1],db.processed)

    #count_articles_per_period = len(raw_data)
    for record in raw_data:
        article_body = record.get('article_body')
        wordsFiltered_bigrams = extract_sentence_bigrams(article_body,record,db.sentences)
        unique_bigrams = filter_unique_bigrams(wordsFiltered_bigrams)
        for bc in unique_bigrams :
            tf = compute_tf(bc,wordsFiltered_bigrams)
            #idf
            #idf = compute_idf(get_data_of_week(period[0],period[1]),bc[0])
            #tfidf = tf * idf
            #print(bc[0])
            obj = {'article_title':record.get('article_title'),'article_urls':record.get('article_urls'),'article_date':record.get('article_date'),'_id':record.get('_id')}
            output = {'bigram':bc[0],'TF-IDF':tf,'week':period[0],'year':period[1],'article':obj}

            db.tfidf.insert(output)


@jit(nopython=True)
def calcul_tfidf(tf,idf):
    return tf*idf

def create_tfidf(period,db):
    raw_data = get_data_of_week(period[0],period[1],db.tfidf)
    for record in raw_data:
        #tf = record.get('tf')
        b = record.get('bigram')
        num_docs_of_bigram = compute_idf(b,period)
        #tfidf = calcul_tfidf(tf,idf)
        record['distinct_articles'] = num_docs_of_bigram
        #record['TF-IDF']=tfidf
        db.tfidf.save(record)




if __name__ == "__main__":
    periods = get_weeks()

    # Can we organize the weeks and year ascendantly?
    #compute(periods)
    for i in range(38,46,2):
    #for i in range(0,len(periods),2):
        p1 = multiprocessing.Process(target=compute_period,args=([i+1,2019],))
        p1.start()
        #p2 = multiprocessing.Process(target=compute_period,args=(periods[i+2],))
        #p2.start()
        compute_period([i,2019])
