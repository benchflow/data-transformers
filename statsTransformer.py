import sys
import json
import urllib.request
import io
import gzip

from dataTransformations import Transformations

from pyspark_cassandra import CassandraSparkContext
from pyspark import SparkConf

# Takes arguments: Spark master, Cassandra host, Minio host, path of the files
sparkMaster = sys.argv[1]
cassandraHost = sys.argv[2]
minioHost = sys.argv[3]
filePath = sys.argv[4]
cassandraKeyspace = "test"

# Set configuration for spark context
conf = SparkConf() \
    .setAppName("PySpark Cassandra Test") \
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
    d["read"] = ob["read"]
    d["usage"] = ob["cpu_stats"]["cpu_usage"]["total_usage"]
    return d

# Calls Spark
query = data.map(createDict)
query.saveToCassandra(cassandraKeyspace, "cpu")