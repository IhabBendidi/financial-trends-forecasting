from pymongo import MongoClient

client = MongoClient('mongodb://dbUser:abcd@cluster0-shard-00-00-7umqv.mongodb.net:27017,cluster0-shard-00-01-7umqv.mongodb.net:27017,cluster0-shard-00-02-7umqv.mongodb.net:27017/test?ssl=true&replicaSet=Cluster0-shard-0&authSource=admin&retryWrites=true&w=majority')

db = client.bmce
remote_collection = db.processed


client2 = MongoClient('localhost', 27017)
db2 = client2.bmce
local_collection = db2.processed


cursor = local_collection.find()
for record in cursor :
    remote_collection.save(record)
