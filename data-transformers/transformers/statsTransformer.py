import sys
import json
#import urllib.request
import urllib2
import io
import gzip
import uuid

from datetime import timedelta

#from dataTransformations import Transformations

from pyspark_cassandra import CassandraSparkContext
from pyspark import SparkConf

# Takes arguments: Spark master, Cassandra host, Minio host, path of the files
sparkMaster = sys.argv[1]
cassandraHost = sys.argv[2]
minioHost = sys.argv[3]
filePath = sys.argv[4]
trialID = sys.argv[5]
experimentID = trialID.split("_")[0]
containerPropertiesID = sys.argv[7]
cassandraKeyspace = "benchflow"
table = "environment_data"
minioPort = "9000"

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
    if (ob["precpu_stats"]["cpu_usage"] != None) and ("total_usage" in ob["precpu_stats"]["cpu_usage"].keys()):
        cpu_percent = 0.0
        cpu_delta = ob["cpu_stats"]["cpu_usage"]["total_usage"] - ob["precpu_stats"]["cpu_usage"]["total_usage"]
        system_delta = ob["cpu_stats"]["system_cpu_usage"] - ob["precpu_stats"]["system_cpu_usage"]
        if system_delta > 0 and cpu_delta > 0:
            cpu_percent = 100.0 * (cpu_delta / float(system_delta * len(ob["cpu_stats"]["cpu_usage"]["percpu_usage"])))
        d["cpu_percent_usage"] = "%.2f" % (cpu_percent)
    d["environment_data_id"] = uuid.uuid1()
    d["trial_id"] = trialID
    d["experiment_id"] = experimentID
    d["container_properties_id"] = containerPropertiesID
    d["read_time"] = ob["read"]
    d["cpu_total_usage"] = long(ob["cpu_stats"]["cpu_usage"]["total_usage"])
    d["cpu_percpu_usage"] = map(long, ob["cpu_stats"]["cpu_usage"]["percpu_usage"])
    d["memory_usage"] = long(ob["memory_stats"]["usage"]/1000000)
    d["memory_max_usage"] = long(ob["memory_stats"]["max_usage"]/1000000)
    d["cpu_throttling_data"] = ob["cpu_stats"]["throttling_data"]
    return d

# Calls Spark
query = data.map(createDict)
query.saveToCassandra(cassandraKeyspace, table, ttl=timedelta(hours=1))