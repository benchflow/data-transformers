import sys
import json
import urllib.request
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
cassandraKeyspace = "benchflow"
table = "environmentdata"

# Gets the benchmark ID from the minio file path
benchmarkID = filePath.split("/")[2]
#benchmarkID = "FHJHKN"

# Set configuration for spark context
conf = SparkConf() \
    .setAppName("Stats Transformer") \
    .setMaster(sparkMaster) \
    .set("spark.cassandra.connection.host", cassandraHost)
sc = CassandraSparkContext(conf=conf)

# Retrieves file from Minio
res = urllib.request.urlopen("http://"+minioHost+":9000/"+filePath)
compressed = io.BytesIO(res.read())
decompressed = gzip.GzipFile(fileobj=compressed)
lines = decompressed.readlines()
data = sc.parallelize(lines)

# Creates a dictionary
def createDict(a):
    ob = json.loads(a.decode())
    d = {}
    d["id"] = uuid.uuid1()
    d["environmentid"] = "something"
    d["time"] = ob["read"]
    d["cputotal"] = int(ob["cpu_stats"]["cpu_usage"]["total_usage"])
    d["memoryused"] = int(ob["memory_stats"]["usage"])
    d["memorytot"] = int(ob["memory_stats"]["max_usage"])
    d["networkin"] = int(ob["network"]["rx_bytes"])
    d["networkout"] = int(ob["network"]["tx_bytes"])
    d["trialid"] = benchmarkID
    return d

# Calls Spark
query = data.map(createDict)
query.saveToCassandra(cassandraKeyspace, table)