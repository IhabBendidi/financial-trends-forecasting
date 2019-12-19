from pymongo import MongoClient
from textblob import Blobber
from textblob_fr import PatternTagger, PatternAnalyzer


client = MongoClient('mongodb://dbUser:abcd@cluster0-shard-00-00-7umqv.mongodb.net:27017,cluster0-shard-00-01-7umqv.mongodb.net:27017,cluster0-shard-00-02-7umqv.mongodb.net:27017/test?ssl=true&replicaSet=Cluster0-shard-0&authSource=admin&retryWrites=true&w=majority')
db = client.bmce
sentence_collection = db.sentences
tb = Blobber(pos_tagger=PatternTagger(), analyzer=PatternAnalyzer())





def get_sentiment(sentence):
    blob = tb(sentence)
    sentiment = blob.sentiment[0]
    return sentiment


def analyze_sentiment(collection):
    raw_data = collection.find({'sentiment':None})
    for record in raw_data:
#        if record.get('sentiment') == None :
        sentence = record.get('sentence')
        sentiment = get_sentiment(sentence)
        record['sentiment'] = sentiment
        collection.save(record)





if __name__ == "__main__":
    analyze_sentiment(sentence_collection)
