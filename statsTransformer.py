import sys
import json
#import urllib.request
import urllib2
import io
import gzip
import uuid

from dataTransformations import Transformations

from pyspark_cassandra import CassandraSparkContext
from pyspark import SparkConf

# Takes arguments: Spark master, Cassandra host, Minio host, path of the files
sparkMaster = sys.argv[1]
cassandraHost = sys.argv[2]
minioHost = sys.argv[3]
filePath = sys.argv[4]
benchmarkID = sys.argv[5]
cassandraKeyspace = "benchflow"
table = "environmentdata"
minioPort = 9000

# Set configuration for spark context
conf = SparkConf() \
    .setAppName("Stats Transformer") \
    .setMaster(sparkMaster) \
    .set("spark.cassandra.connection.host", cassandraHost)
sc = CassandraSparkContext(conf=conf)

# Retrieves file from Minio
#res = urllib.request.urlopen("http://"+minioHost+":9000/"+filePath)
#compressed = io.BytesIO(res.read())
#decompressed = gzip.GzipFile(fileobj=compressed)
#lines = decompressed.readlines()
#data = sc.parallelize(lines)

res = urllib2.urlopen("http://"+minioHost+":"+minioPort+"/"+filePath)
compressed = io.BytesIO(res.read())
decompressed = gzip.GzipFile(fileobj=compressed)
lines = decompressed.readlines()
data = sc.parallelize(lines)

# Creates a dictionary
def createDict(a):
    ob = json.loads(a.decode())
    d = {}
    d["id"] = uuid.uuid1()
    d["environmentid"] = benchmarkID
    d["time"] = ob["read"]
    d["cputotal"] = long(ob["cpu_stats"]["cpu_usage"]["total_usage"])
    d["memoryused"] = long(ob["memory_stats"]["usage"])
    d["memorytot"] = long(ob["memory_stats"]["max_usage"])
    #d["networkin"] = int(ob["network"]["rx_bytes"])
    #d["networkout"] = int(ob["network"]["tx_bytes"])
    d["trialid"] = "Something"
    return d

# Calls Spark
query = data.map(createDict)
query.saveToCassandra(cassandraKeyspace, table)