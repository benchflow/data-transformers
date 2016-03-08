import sys
import json
import yaml
#import urllib.request
import urllib2
import io
import gzip
import numpy as np

import uuid

from datetime import timedelta

#from dataTransformations import Transformations

from pyspark_cassandra import CassandraSparkContext
from pyspark import SparkConf
from pyspark import SparkFiles
#from builtins import True

# Takes arguments: Spark master, Cassandra host, Minio host, path of the file
sparkMaster = sys.argv[1]
cassandraHost = sys.argv[2]
minioHost = sys.argv[3]
filePath = sys.argv[4]
trialID = sys.argv[5]
experimentID = trialID.split("_")[0]
SUTName = sys.argv[6]
containerPropertiesID = sys.argv[7]
cassandraKeyspace = "benchflow"
minioPort = "9000"

def filterMock(a):
    for e in a:
        if limit in e:
            return False
    return True

# This function creates a dictionary that acts as query to pass to Cassandra
def createDic(a):
    d = {}
    if(a[0] == schema[0]):
        d["trial_id"] = trialID
        d["experiment_id"] = experimentID
        d["process_instance_id"] = uuid.uuid1()
        return d
    for i in indexes:
        col = conf["column_mapping"][i]
        if conf.get("column_transformation") != None:
            t = conf["column_transformation"].get(i)
        else:
            t = None
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
    d["process_instance_id"] = uuid.uuid1()
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
#confPath = SparkFiles.get("conf.json")
confPath = SparkFiles.get(SUTName+".data-transformers.yml")
with open(confPath) as f:
    #maps = json.load(f)
    maps = yaml.load(f)
    mappings = maps["settings"]
    limit = maps["limit_process_string_id"]
    
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
    data = sc.parallelize(lines[1:])
    
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
    schema = lines[0].decode().split(",")
    print(schema)
    
    # Fills the indexes dictionary with the correct indexes
    for i in range(len(schema)):
        for c in conf["column_mapping"]:
            if schema[i].replace('"', '') == c:
                indexes[schema[i].replace('"', '')] = i
                
    print(indexes)
    
    # Uses Spark to map lines to Cassandra queries
    query = data.map(lambda line: line.decode().split(",")).filter(filterMock).map(createDic)
    query.saveToCassandra(cassandraKeyspace, conf["dest_table"], ttl=timedelta(hours=1))

# Save Database size    
res = urllib2.urlopen("http://"+minioHost+":"+minioPort+"/"+filePath+"/database_table_sizes.csv.gz")
compressed = io.BytesIO(res.read())
decompressed = gzip.GzipFile(fileobj=compressed)
lines = decompressed.readlines()
lines = lines[1:]
data = sc.parallelize(lines)

# TODO: Use received dbms
query = data.map(lambda line: line.decode().split(",")).map(lambda line: {"experiment_id":experimentID, "trial_id":trialID, "dbms":"TEMP", "database_name": line[0].replace('"', ''), "size":long(line[1].replace('"', ''))})
query.saveToCassandra(cassandraKeyspace, "database_sizes", ttl=timedelta(hours=1))