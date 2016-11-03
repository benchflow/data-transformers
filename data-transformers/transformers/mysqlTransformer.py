import sys
import json
import yaml
import urllib2
import io
import gzip
import uuid
import dateutil.parser as dateparser

from datetime import timedelta

#Function for filtering mock values
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
    #Not recording values if the line is the schema
    if(a[0] == schema[0]):
        d["trial_id"] = trialID
        d["experiment_id"] = experimentID
        return d
    #Iterate over the indexes and map to the corresponding column as defined in the configuration file
    for i in indexes:
        col = conf["column_mapping"][i]
        if conf.get("column_transformation") != None:
            t = conf["column_transformation"].get(i)
        else:
            t = None
        #If defined as NULL, save it as type None
        if(a[indexes[i]].replace('"', '') == "NULL"):
            d[col] = None
            continue
        #Apply transformation function if specified
        if(t == None):
            d[col] = convertType(a[indexes[i]].replace('"', ''), i, types)
        else:
            d[col] = eval("transformations."+t)(convertType(a[indexes[i]].replace('"', ''), i, types))
    d["trial_id"] = trialID
    d["experiment_id"] = experimentID
    #If duration not present, use start time and end time to get the duration
    if "duration" not in d.keys() and "end_time" in d.keys() and "start_time" in d.keys():
        d["duration"] = (dateparser.parse(d["end_time"]) - dateparser.parse(d["start_time"])).seconds
    return d

#Function to convert the data, which are strings to start, based on the type defined in the database schema
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

#Function to retrieve the raw data from Minio
def getFromMinio(minioHost, minioPort, accessKey, secretKey, bucket, path):
    from commons import getFromMinio
    return getFromMinio(minioHost, minioPort, accessKey, secretKey, bucket, path)

#Function that marks N initial processes to be ignored
def cutNInitialProcesses(dataRDD, nToIgnore):
    from pyspark_cassandra import CassandraSparkContext
    from pyspark_cassandra import RowFormat
    from pyspark import SparkConf
    
    #Function to mark a process to be ignored if the time is lower than a maximum time
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
    
    #Get all distinct processes
    processes = dataRDD.map(lambda r: r["process_definition_id"]) \
            .distinct() \
            .collect()
    
    #For all processes, mark to ignore if start time lower than the start time of the Nth process
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

#Function to mark construct to be ignored if belonging to processes that need to be ignored
def cutConstructs(dataRDD, processesToIgnore):
    from pyspark_cassandra import CassandraSparkContext
    from pyspark_cassandra import RowFormat
    from pyspark import SparkConf
    
    #Function that marks a construct to be ignored if belonging to a process that needs to be ignored
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
    # SUTName = str(args["sut_name"])
    configFile = str(args["config_file"])
    partitionsPerCore = 5
    
    # Set configuration for spark context
    conf = SparkConf().setAppName("MYSQL Transformer")
    sc = CassandraSparkContext(conf=conf)
    
    # Retrieve the configuration file that was sent to spark
    confPath = SparkFiles.get(configFile)
    with open(confPath) as f:
        transformerConfiguration = yaml.load(f)
        mappings = transformerConfiguration["settings"]
        limit = transformerConfiguration["limit_process_string_id"]
        nProcessesToIgnore = transformerConfiguration["n_processes_to_ignore"]
    
    #Read the mappings of columns to columns for each defined table in the conf file 
    mappingsDict = {}
    for conf in mappings:
        mappingsDict[conf["dest_table"]] = conf
    mappings = [mappingsDict["process"], mappingsDict["construct"]]
    
     #For each table, perform the mappings of columns
    for conf in mappings:
        #Get Data from Minio and parallelize it with Spark
        lines = getFromMinio(minioHost, minioPort, minioAccessKey, minioSecretKey, fileBucket, filePath+"/"+conf["src_table"]+".csv.gz").readlines()
        data = sc.parallelize(lines[1:], sc.defaultParallelism * partitionsPerCore)
        
        #Get schema from Minio and save types of the data
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
        
        #Mark processes and construct to be ignored        
        cutProcesses = []
        if conf["dest_table"] == "process":
            query = cutNInitialProcesses(query, nProcessesToIgnore)
            cutProcesses = query.filter(lambda a: a["to_ignore"] is True).map(lambda a: a["source_process_instance_id"]).collect()
        elif conf["dest_table"] == "construct":
            query = cutConstructs(query, cutProcesses)
        
        #Save to Cassandra
        query.saveToCassandra(cassandraKeyspace, conf["dest_table"])
    
    # Save Database size    
    lines = getFromMinio(minioHost, minioPort, minioAccessKey, minioSecretKey, fileBucket, filePath+"/"+"database_table_sizes.csv.gz").readlines()
    data = sc.parallelize(lines[1:], sc.defaultParallelism * partitionsPerCore)
    
    # TODO: Use received dbms
    query = data.map(lambda line: line.decode().split(",")).map(lambda line: {"experiment_id":experimentID, "trial_id":trialID, "dbms":"MySQL", "database_name": line[0].replace('"', ''), "size":long(line[1].replace('"', ''))})
    query.saveToCassandra(cassandraKeyspace, "database_sizes")
    
if __name__ == '__main__':
    main()