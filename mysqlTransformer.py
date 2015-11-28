import sys
import json
import urllib.request
import io
import gzip

from dataTransformations import Transformations

from pyspark_cassandra import CassandraSparkContext
from pyspark import SparkConf
from pyspark import SparkFiles

# This function creates a dictionary that acts as query to pass to Cassandra
def createDic(a):
    d = {}
    if(a[0] == schema[0]):
        return {"id":"0", "duration":"0"}
    for i in indexes:
        col = conf["column_mapping"][i]
        t = conf["column_transformation"].get(i)
        if(t == None):
            d[col] = a[indexes[i]]
        else:
            tf = getattr(Transformations, t)
            d[col] = tf(a[indexes[i]])
    return d

# Takes arguments: Spark master, Cassandra host, Minio host, path of the file
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

# Retrieve the configuration file that was sent to spark
confPath = SparkFiles.get("conf.json")
with open(confPath) as f:
    maps = json.load(f)
    mappings = mappings["settings"]
    
for conf in mappings:
    # Retrieves the file from Minio and parallelize it for Spark
    res = urllib.request.urlopen("http://"+minioHost+"/"+filePath+"_"+conf["src_table"]+".gz")
    compressed = io.BytesIO(res.read())
    decompressed = gzip.GzipFile(fileobj=compressed)
    lines = decompressed.readlines()
    data = sc.parallelize(lines)
    
    # Sets up the dictionary to keep track of the index of a certain data in a CSV file line
    indexes = {}
    schema = data.first().decode().split(",")
    
    # Fills the indexes dictionary with the correct indexes
    for i in range(len(schema)):
        for c in conf["column_mapping"]:
            if schema[i].replace('"', '') == c:
                indexes[schema[i].replace('"', '')] = i
    
    # Uses Spark to map lines to Cassandra queries
    query = data.map(lambda line: line.decode().split(",")).map(createDic)
    query.saveToCassandra(cassandraKeyspace, conf["dest_table"])