import sys
import json
import yaml
import urllib2
import io
import gzip
import uuid
import dateutil.parser as dateparser

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
    if "duration" not in d.keys() and "end_time" in d.keys() and "start_time" in d.keys():
        d["duration"] = (dateparser.parse(d["end_time"]) - dateparser.parse(d["start_time"])).seconds
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

def getFromMinio(minioHost, minioPort, accessKey, secretKey, bucket, path):
    from commons import getFromMinio
    return getFromMinio(minioHost, minioPort, accessKey, secretKey, bucket, path)
        
def cutNInitialProcesses(dataRDD, nToIgnore):
    from pyspark_cassandra import CassandraSparkContext
    from pyspark_cassandra import RowFormat
    from pyspark import SparkConf
    
    def markToIgnore(e, maxTime, proc):
        if e['process_definition_id'] == proc and dateparser.parse(e["start_time"]) <= maxTime:
            e["to_ignore"] = True
            return e
        elif not "to_ignore" in e.keys(): 
            e["to_ignore"] = False
            return e
        else:
            return e
    
    if dataRDD.isEmpty():
        return []
    
    processes = dataRDD.map(lambda r: r["process_definition_id"]) \
            .distinct() \
            .collect()
    
    for p in processes:
        time = dataRDD.filter(lambda r: r["process_definition_id"] == p) \
            .map(lambda r: (dateparser.parse(r["start_time"]), 0)) \
            .sortByKey(1, 1) \
            .take(nToIgnore)
        if len(time) < nToIgnore:
            continue
        else:
            time = time[-1][0]
        
        dataRDD = dataRDD.map(lambda e: markToIgnore(e, time, p))
    return dataRDD

def cutConstructs(dataRDD, processesToIgnore):
    from pyspark_cassandra import CassandraSparkContext
    from pyspark_cassandra import RowFormat
    from pyspark import SparkConf
    
    def markToIgnore(e):
        if e["source_process_instance_id"] in processesToIgnore:
            e["to_ignore"] = True
            return e
        elif not "to_ignore" in e.keys(): 
            e["to_ignore"] = False
            return e
        else:
            return e
    
    return dataRDD.map(markToIgnore)

def main():
    from pyspark_cassandra import CassandraSparkContext
    from pyspark import SparkConf
    from pyspark import SparkFiles
    from pyspark import StorageLevel
    
    # Takes arguments
    args = json.loads(sys.argv[1])
    cassandraKeyspace = str(args["cassandra_keyspace"])
    minioHost = str(args["minio_host"])
    minioPort = str(args["minio_port"])
    minioAccessKey = str(args["minio_access_key"])
    minioSecretKey = str(args["minio_secret_key"])
    fileBucket = str(args["file_bucket"])
    filePath = str(args["file_path"])
    trialID = str(args["trial_id"])
    experimentID = str(args["experiment_id"])
    SUTName = str(args["sut_name"])
    
    # Set configuration for spark context
    conf = SparkConf().setAppName("MYSQL Transformer")
    sc = CassandraSparkContext(conf=conf)
    
    # Retrieve the configuration file that was sent to spark
    confPath = SparkFiles.get(SUTName+".data-transformers.yml")
    with open(confPath) as f:
        transformerConfiguration = yaml.load(f)
        mappings = transformerConfiguration["settings"]
        limit = transformerConfiguration["limit_process_string_id"]
        nProcessesToIgnore = transformerConfiguration["n_processes_to_ignore"]
        
    mappingsDict = {}
    for conf in mappings:
        mappingsDict[conf["dest_table"]] = conf
    mappings = [mappingsDict["process"], mappingsDict["construct"]]
        
    for conf in mappings:
        lines = getFromMinio(minioHost, minioPort, minioAccessKey, minioSecretKey, fileBucket, filePath+"/"+conf["src_table"]+".csv.gz").readlines()
        data = sc.parallelize(lines[1:])
        
        lines2 = getFromMinio(minioHost, minioPort, minioAccessKey, minioSecretKey, fileBucket, filePath+"/"+conf["src_table"]+"_schema.csv.gz").readlines()   
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
        query = data.map(lambda line: line.decode().split(",")) \
                    .filter(lambda a: filterMock(a, limit)) \
                    .map(lambda a: createDic(a, trialID, experimentID, conf, types, schema, indexes))
                    
        cutProcesses = []
        if conf["dest_table"] == "process":
            query = cutNInitialProcesses(query, nProcessesToIgnore)
            cutProcesses = query.filter(lambda a: a["to_ignore"] is True).map(lambda a: a["source_process_instance_id"]).collect()
        elif conf["dest_table"] == "construct":
            query = cutConstructs(query, cutProcesses)
        
        query.saveToCassandra(cassandraKeyspace, conf["dest_table"], ttl=timedelta(hours=1))
    
    # Save Database size    
    lines = getFromMinio(minioHost, minioPort, minioAccessKey, minioSecretKey, fileBucket, filePath+"/"+"database_table_sizes.csv.gz").readlines()
    data = sc.parallelize(lines[1:])
    
    # TODO: Use received dbms
    query = data.map(lambda line: line.decode().split(",")).map(lambda line: {"experiment_id":experimentID, "trial_id":trialID, "dbms":SUTName, "database_name": line[0].replace('"', ''), "size":long(line[1].replace('"', ''))})
    query.saveToCassandra(cassandraKeyspace, "database_sizes", ttl=timedelta(hours=1))
    
if __name__ == '__main__':
    main()