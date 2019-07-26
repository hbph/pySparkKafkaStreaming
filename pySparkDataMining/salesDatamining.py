import sys
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pymongo import MongoClient

import json

def sendRecord(data):
    connection  = MongoClient()
    country = data[0]
    amount = data[1]
    test_db        = connection.get_database('testdb')
    wordcount_coll = test_db.get_collection('salesDtls')
    wordcount_coll.update({"country": country}, {"$inc": {"totalRevenue": round(float(amount),2)} }, upsert=True)
    connection.close()

if __name__ == "__main__":
	
    sc = SparkContext(appName="SalesTotalRevenueFilterByCountry")
    # Intialize the log4j for logging 
    log4jLogger = sc._jvm.org.apache.log4j
    # get the logger
    log = log4jLogger.LogManager.getLogger(__name__)
    
    # Refersh the stream context for every 10secs
    ssc = StreamingContext(sc, 5)
    
    # Read the topic from command line input
    brokers, topic = sys.argv[1:]    
    kvs = KafkaUtils.createDirectStream(ssc, [topic],{"metadata.broker.list": brokers})
    
    # Read the json and extract the country and its sales
    dstream = kvs.map(lambda x: json.loads(x[1])).map(lambda x: (x.get('country'), x.get('totalRevenue')))    
    
    # http://spark.apache.org/docs/latest/streaming-programming-guide.html#design-patterns-for-using-foreachrdd
    log.info("Save the sales details to MongoDB !!!")
    dstream.foreachRDD(lambda rdd: rdd.foreach(sendRecord))    
    
    # start the analytics computation and wait for the computation to terminate.
    ssc.start()
    ssc.awaitTermination()
    
