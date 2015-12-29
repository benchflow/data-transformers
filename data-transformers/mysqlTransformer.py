import sys
import json
#import urllib.request
import urllib2
import io
import gzip

from dataTransformations import Transformations

from pyspark_cassandra import CassandraSparkContext
from pyspark import SparkConf
from pyspark import SparkFiles

# Takes arguments: Spark master, Cassandra host, Minio host, path of the file
sparkMaster = sys.argv[1]
cassandraHost = sys.argv[2]
minioHost = sys.argv[3]
filePath = sys.argv[4]
cassandraKeyspace = "benchflow"
minioPort = 9000

# This function creates a dictionary that acts as query to pass to Cassandra
def createDic(a):
    d = {}
    if(a[0] == schema[0]):
        return None
    for i in indexes:
        col = conf["column_mapping"][i]
        t = conf["column_transformation"].get(i)
        if(t == None):
            d[col] = convertType(a[indexes[i]], col)
        else:
            tf = getattr(Transformations, t)
            d[col] = convertType(tf(a[indexes[i]]), col)
    return d

def convertType(element, column):
    if types[column].startswith("varchar"):
        return element
    if types[column].startswith("int"):
        return int(element)
    raise ValueError

# Set configuration for spark context
conf = SparkConf() \
    .setAppName("MYSQL Transformer") \
    .setMaster(sparkMaster) \
    .set("spark.cassandra.connection.host", cassandraHost)

sc = CassandraSparkContext(conf=conf)

# Retrieve the configuration file that was sent to spark
confPath = SparkFiles.get("conf.json")
with open(confPath) as f:
    maps = json.load(f)
    mappings = maps["settings"]
    
for conf in mappings:
    # Retrieves file from Minio
    #res = urllib.request.urlopen("http://"+minioHost+":"+minioPort+"/"+filePath+"/"+conf["src_table"]+"csv.gz")
    #compressed = io.BytesIO(res.read())
    #decompressed = gzip.GzipFile(fileobj=compressed)
    #lines = decompressed.readlines()
    #data = sc.parallelize(lines)
    
    res = urllib2.urlopen("http://"+minioHost+":"+minioPort+"/"+filePath+"/"+conf["src_table"]+".csv.gz")
    compressed = io.BytesIO(res.read())
    decompressed = gzip.GzipFile(fileobj=compressed)
    lines = decompressed.readlines()
    data = sc.parallelize(lines)
    
    res = urllib2.urlopen("http://"+minioHost+":"+minioPort+"/"+filePath+"/"+conf["src_table"]+"_schema.csv.gz")
    compressed = io.BytesIO(res.read())
    decompressed = gzip.GzipFile(fileobj=compressed)
    lines = decompressed.readlines()
    
    types = {}
    for line in lines:
        l = line.decode().split(",")
        if l[0] != "Field":
            types[l[0]] = l[1]
    
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