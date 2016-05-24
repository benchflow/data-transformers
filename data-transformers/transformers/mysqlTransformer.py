import sys
import json
import yaml
import urllib2
import io
import gzip

import uuid

from datetime import timedelta

def filterMock(a, limit):
    for e in a:
        if limit in e:
            return False
    return True

# This function creates a dictionary that acts as query to pass to Cassandra
def createDic(a, trialID, experimentID, conf, types, schema, indexes):
    from dataTransformations import Transformations
    transformations = Transformations()
    d = {}
    if(a[0] == schema[0]):
        d["trial_id"] = trialID
        d["experiment_id"] = experimentID
        d["process_instance_id"] = uuid.uuid1().urn[9:]
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
            d[col] = convertType(a[indexes[i]].replace('"', ''), i, types)
        else:
            d[col] = eval("transformations."+t)(convertType(a[indexes[i]].replace('"', ''), i, types))
    d["trial_id"] = trialID
    d["experiment_id"] = experimentID
    d["process_instance_id"] = uuid.uuid1().urn[9:]
    return d

def convertType(element, column, types):
    if types[column].startswith("varchar"):
        return element
    if types[column].startswith("uuid"):
        return element
    if types[column].startswith("int"):
        return int(element)
    if types[column].startswith("bigint"):
        return long(element)
    return element

def getFromMinio(url):
    from commons import getFromUrl
    return getFromUrl(url)

def main():
    from pyspark_cassandra import CassandraSparkContext
    from pyspark import SparkConf
    from pyspark import SparkFiles
    
    # Takes arguments: Spark master, Cassandra host, Minio host, path of the file
    minioHost = sys.argv[1]
    filePath = sys.argv[2]
    trialID = sys.argv[3]
    experimentID = sys.argv[4]
    SUTName = sys.argv[5]
    
    # Set configuration for spark context
    conf = SparkConf().setAppName("MYSQL Transformer")
    sc = CassandraSparkContext(conf=conf)
    
    # Retrieve the configuration file that was sent to spark
    #confPath = SparkFiles.get("data-transformers.yml")
    #with open(confPath) as f:
    #    transformerConfiguration = yaml.load(f)
    #    cassandraKeyspace = transformerConfiguration["cassandra_keyspace"]
    #    minioPort = transformerConfiguration["minio_port"]
    
    cassandraKeyspace = "benchflow"
    minioPort = "9000"
    
    confPath = SparkFiles.get(SUTName+".data-transformers.yml")
    with open(confPath) as f:
        transformerConfiguration = yaml.load(f)
        mappings = transformerConfiguration["settings"]
        limit = transformerConfiguration["limit_process_string_id"]
        
    for conf in mappings:
        #res = urllib2.urlopen("http://"+minioHost+":"+minioPort+"/"+filePath+"/"+conf["src_table"]+".csv.gz")
        #compressed = io.BytesIO(res.read())
        #decompressed = gzip.GzipFile(fileobj=compressed)
        #lines = decompressed.readlines()
        lines = getFromMinio("http://"+minioHost+":"+minioPort+"/"+filePath+"/"+conf["src_table"]+".csv.gz")
        data = sc.parallelize(lines[1:])
        
        #res2 = urllib2.urlopen("http://"+minioHost+":"+minioPort+"/"+filePath+"/"+conf["src_table"]+"_schema.csv.gz")
        #compressed2 = io.BytesIO(res2.read())
        #decompressed2 = gzip.GzipFile(fileobj=compressed2)
        #lines2 = decompressed2.readlines()
        lines2 = getFromMinio("http://"+minioHost+":"+minioPort+"/"+filePath+"/"+conf["src_table"]+"_schema.csv.gz")
        
        types = {}
        for line in lines2:
            line = line.decode()
            l = line.split(",")
            if l[0].replace('"', '') != "Field":
                types[l[0].replace('"', '')] = l[1]
        
        # Sets up the dictionary to keep track of the index of a certain data in a CSV file line
        indexes = {}
        schema = lines[0].decode().split(",")
        
        # Fills the indexes dictionary with the correct indexes
        for i in range(len(schema)):
            for c in conf["column_mapping"]:
                if schema[i].replace('"', '') == c:
                    indexes[schema[i].replace('"', '')] = i
        
        # Uses Spark to map lines to Cassandra queries
        f1 = lambda a: createDic(a, trialID, experimentID, conf, types, schema, indexes)
        f2 = lambda a: filterMock(a, limit)
        query = data.map(lambda line: line.decode().split(",")).filter(f2).map(f1)
        query.saveToCassandra(cassandraKeyspace, conf["dest_table"], ttl=timedelta(hours=1))
    
    # Save Database size    
    #res = urllib2.urlopen("http://"+minioHost+":"+minioPort+"/"+filePath+"/database_table_sizes.csv.gz")
    #compressed = io.BytesIO(res.read())
    #decompressed = gzip.GzipFile(fileobj=compressed)
    #lines = decompressed.readlines()
    lines = getFromMinio("http://"+minioHost+":"+minioPort+"/"+filePath+"/database_table_sizes.csv.gz")
    data = sc.parallelize(lines[1:])
    
    # TODO: Use received dbms
    query = data.map(lambda line: line.decode().split(",")).map(lambda line: {"experiment_id":experimentID, "trial_id":trialID, "dbms":SUTName, "database_name": line[0].replace('"', ''), "size":long(line[1].replace('"', ''))})
    query.saveToCassandra(cassandraKeyspace, "database_sizes", ttl=timedelta(hours=1))
    
if __name__ == '__main__':
    main()