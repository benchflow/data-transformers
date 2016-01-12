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
trialID = sys.argv[5]
experimentID = trialID.split("_")[0]
containerPropertiesID = sys.argv[6]
cassandraKeyspace = "benchflow"
minioPort = "9000"

# This function creates a dictionary that acts as query to pass to Cassandra
def createDic(a):
    d = {}
    if(a[0] == schema[0]):
        d["trial_id"] = trialID
        d["experiment_id"] = experimentID
        d["process_instance_id"] = "000020f8-5c9c-11e5-8cb8-5af0dffbf049"
        return d
    for i in indexes:
        col = conf["column_mapping"][i]
        t = conf["column_transformation"].get(i)
        if(a[indexes[i]].replace('"', '') == "NULL"):
            d[col] = None
            continue
        if(t == None):
            d[col] = convertType(a[indexes[i]].replace('"', ''), i)
        else:
            tf = getattr(Transformations, t)
            d[col] = convertType(a[indexes[i]].replace('"', ''), i)
            #d[col] = convertType(tf(a[indexes[i]]), i)
    d["trial_id"] = trialID
    d["experiment_id"] = experimentID
    return d

def convertType(element, column):
    if types[column].startswith("varchar"):
        return element
    if types[column].startswith("uuid"):
        return element
    if types[column].startswith("int"):
        return int(element)
    return element

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
    
    res2 = urllib2.urlopen("http://"+minioHost+":"+minioPort+"/"+filePath+"/"+conf["src_table"]+"_schema.csv.gz")
    compressed2 = io.BytesIO(res2.read())
    decompressed2 = gzip.GzipFile(fileobj=compressed2)
    lines2 = decompressed2.readlines()
    
    types = {}
    for line in lines2:
        line = line.decode()
        l = line.split(",")
        if l[0].replace('"', '') != "Field":
            types[l[0].replace('"', '')] = l[1]
            
    print(types)
    
    # Sets up the dictionary to keep track of the index of a certain data in a CSV file line
    indexes = {}
    schema = data.first().decode().split(",")
    print(schema)
    
    # Fills the indexes dictionary with the correct indexes
    for i in range(len(schema)):
        for c in conf["column_mapping"]:
            if schema[i].replace('"', '') == c:
                indexes[schema[i].replace('"', '')] = i
                
    print(indexes)
    
    # Uses Spark to map lines to Cassandra queries
    query = data.map(lambda line: line.decode().split(",")).map(createDic)
    query.saveToCassandra(cassandraKeyspace, conf["dest_table"], ttl=timedelta(hours=1))