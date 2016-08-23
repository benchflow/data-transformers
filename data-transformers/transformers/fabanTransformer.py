import sys
import json
import uuid
import xml.etree.ElementTree as xml
import threading
from datetime import timedelta
import dateutil.parser as dateparser

def createRunInfoQuery(data, trialID, experimentID, host, runStatus):
    query = {}
    query["trial_id"] = trialID
    query["experiment_id"] = experimentID
    query["host"] = host
    benchSum = data.find("benchSummary")
    query["name"] = benchSum.attrib["name"]
    query["version"] = benchSum.attrib["version"]
    query["run_id"] = benchSum.find("runId").text
    query["start_time"] = benchSum.find("startTime").text
    query["end_time"] = benchSum.find("endTime").text
    query["duration"] = (dateparser.parse(query["end_time"]) - dateparser.parse(query["start_time"])).seconds
    query["metric_unit"] = benchSum.find("metric").attrib["unit"]
    query["metric_value"] = benchSum.find("metric").text
    query["passed"] = benchSum.find("passed").text
    query["status"] = runStatus
    return query

def createDriverSummaryQuery(data, trialID, experimentID, host):
    queries = []
    for driver in data.findall("driverSummary"):
        query = {}
        query["trial_id"] = trialID
        query["experiment_id"] = experimentID
        query["host"] = host
        query["name"] = driver.attrib["name"]
        query["metric_unit"] = driver.find("metric").attrib["unit"]
        query["metric_value"] = driver.find("metric").text
        query["start_time"] = driver.find("startTime").text
        query["end_time"] = driver.find("endTime").text
        query["total_ops_unit"] = driver.find("totalOps").attrib["unit"]
        query["total_ops_value"] = driver.find("totalOps").text
        query["users"] = driver.find("users").text
        query["rtxtps"] = driver.find("rtXtps").text
        query["passed"] = driver.find("passed").text
        query["mix_allowed_deviation"] = driver.find("mix").attrib["allowedDeviation"]
        query["response_times_unit"] = driver.find("responseTimes").attrib["unit"]
        queries.append(query)
    return queries

def createDriverMixQuery(data, trialID, experimentID, host):
    queries = []
    for driver in data.findall("driverSummary"):
        for operation in driver.find("mix"):
            query = {}
            query["trial_id"] = trialID
            query["experiment_id"] = experimentID
            query["host"] = host
            query["driver_name"] = driver.attrib["name"]
            query["op_name"] = operation.attrib["name"]
            query["allowed_deviation"] = driver.find("mix").attrib["allowedDeviation"]
            query["successes"] = operation.find("successes").text
            query["failures"] = operation.find("failures").text
            query["mix"] = operation.find("mix").text
            query["required_mix"] = operation.find("requiredMix").text
            query["passed"] = operation.find("passed").text
            queries.append(query)
    return queries

def createDriverResponseTimesQuery(data, trialID, experimentID, host):
    queries = []
    for driver in data.findall("driverSummary"):
        for operation in driver.find("responseTimes"):
            for stat in operation:
                if "passed" in stat.tag:
                    continue
                query = {}
                query["trial_id"] = trialID
                query["experiment_id"] = experimentID
                query["host"] = host
                query["driver_name"] = driver.attrib["name"]
                query["op_name"] = operation.attrib["name"]
                if stat.tag == "percentile":
                    query["stat_name"] = stat.tag+"_"+stat.attrib["nth"]+"_"+stat.attrib["suffix"]
                else:
                    query["stat_name"] = stat.tag
                query["stat_value"] = stat.text
                query["passed"] = operation.find("passed").text
                queries.append(query)
    return queries

def createDriverDelayTimesQuery(data, trialID, experimentID, host):
    queries = []
    for driver in data.findall("driverSummary"):
        for operation in driver.find("delayTimes"):
            query = {}
            query["trial_id"] = trialID
            query["experiment_id"] = experimentID
            query["host"] = host
            query["driver_name"] = driver.attrib["name"]
            query["op_name"] = operation.attrib["name"]
            query["type"] = operation.attrib["type"]
            query["targeted_avg"] = operation.find("targetedAvg").text
            query["actual_avg"] = operation.find("actualAvg").text
            query["min"] = operation.find("min").text
            query["max"] = operation.find("max").text
            query["passed"] = operation.find("passed").text
            queries.append(query)
    return queries

def createDriverCustomStatsQuery(data, trialID, experimentID, host):
    queries = []
    for driver in data.findall("driverSummary"):
        if driver.find("customStats") is None:
            return queries
        for stat in driver.find("customStats"):
            query = {}
            query["trial_id"] = trialID
            query["experiment_id"] = experimentID
            query["host"] = host
            query["stat_name"] = driver.find("customStats").attrib["name"]
            query["driver_name"] = driver.attrib["name"]
            query["description"] = stat.find("description").text
            query["target"] = stat.find("target").text
            query["result"] = stat.find("result").text
            queries.append(query)
    return queries

def createDetailsQuery(data, trialID, experimentID, host):
    firstSectionIndex = 0
    for index, value in enumerate(data):
        if "Section" in value and "Benchmark" not in value:
            firstSectionIndex = index
            break
    data = data[firstSectionIndex:]   
    queries = []
    currentOps = []
    currentSection = ""
    timeUnit = ""
    for line in data:
        if "Section" in line:
            currentSection = line.split(":")[1]
            if "(" in currentSection.split()[-1] and ")" in currentSection.split()[1]:
                timeUnit = line.split()[1]
        elif "Time" in line:
            if "(" in line.split()[1] and ")" in line.split()[1]:
                timeUnit = line.split()[1]
                currentOps = line.split()[2:]
            else:
                currentOps = line.split()[1:]
        elif len(line.split()) > 0 and "--------" not in line and "Display" not in line:
            val = line.split()
            for index, value in enumerate(currentOps):
                query = {}
                query["id"] = uuid.uuid1().urn[9:]
                query["trial_id"] = trialID
                query["experiment_id"] = experimentID
                query["host"] = host
                query["time"] = val[0]
                query["time_unit"] = timeUnit
                query["value"] = val[index+1]
                query["section"] = currentSection
                query["op_name"] = value
                queries.append(query)
    return queries

def getMinioPaths(minioHost, minioPort, accessKey, secretKey, bucket, path):
    from commons import getMinioPaths
    return getMinioPaths(minioHost, minioPort, accessKey, secretKey, bucket, path)

def getFromMinio(minioHost, minioPort, accessKey, secretKey, bucket, path):
    from commons import getFromMinio
    return getFromMinio(minioHost, minioPort, accessKey, secretKey, bucket, path)
        
def getHostName(path):
    fileName = path.split("/")[-1]
    host = "aggregate"
    if len(fileName.split(".")) > 3:
        host = fileName.split(".")[2]
    return host

def main():
    from pyspark_cassandra import CassandraSparkContext
    from pyspark import SparkConf
    from pyspark import SparkFiles
    
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
    partitionsPerCore = 5
    
    # Set configuration for spark context
    conf = SparkConf().setAppName("Faban Transformer")
    sc = CassandraSparkContext(conf=conf)
    
    minioPaths = getMinioPaths(minioHost, minioPort, minioAccessKey, minioSecretKey, fileBucket, filePath)
    
    runStatus = None
    for path in minioPaths:
        if "resultinfo" in path:
            data = getFromMinio(minioHost, minioPort, minioAccessKey, minioSecretKey, fileBucket, path)
            data = data.readline()
            runStatus = data.split("\t")[0]
            break
    
    for path in minioPaths:
        if "summary.xml" in path:     
            host = getHostName(path)
            
            data = getFromMinio(minioHost, minioPort, minioAccessKey, minioSecretKey, fileBucket, path)
            
            data = data.read()
            dataXML = xml.fromstring(data)
            
            def fA():
                query = createRunInfoQuery(dataXML, trialID, experimentID, host, runStatus)
                query = sc.parallelize([query])
                query.saveToCassandra(cassandraKeyspace, "faban_run_info")
            
            def fB():
                query = createDriverSummaryQuery(dataXML, trialID, experimentID, host)
                query = sc.parallelize(query)
                query.saveToCassandra(cassandraKeyspace, "faban_driver_summary")
                
            def fC():
                query = createDriverMixQuery(dataXML, trialID, experimentID, host)
                query = sc.parallelize(query)
                query.saveToCassandra(cassandraKeyspace, "faban_driver_mix")
                
            def fD():
                query = createDriverResponseTimesQuery(dataXML, trialID, experimentID, host)
                query = sc.parallelize(query)
                query.saveToCassandra(cassandraKeyspace, "faban_driver_response_times")
                
            def fE():
                query = createDriverDelayTimesQuery(dataXML, trialID, experimentID, host)
                query = sc.parallelize(query)
                query.saveToCassandra(cassandraKeyspace, "faban_driver_delay_times")
                
            def fF():
                query = createDriverCustomStatsQuery(dataXML, trialID, experimentID, host)
                if len(query) != 0:
                    query = sc.parallelize(query)
                    query.saveToCassandra(cassandraKeyspace, "faban_driver_custom_stats")
                
            tA = threading.Thread(target=fA)
            tB = threading.Thread(target=fB)
            tC = threading.Thread(target=fC)
            tD = threading.Thread(target=fD)
            tE = threading.Thread(target=fE)
            tF = threading.Thread(target=fF)
            tA.start()
            tB.start()
            tC.start()
            tD.start()
            tE.start()
            tF.start()
            tA.join()
            tB.join()
            tC.join()
            tD.join()
            tE.join()
            tF.join()
        
        elif "detail.xan" in path:
            host = getHostName(path)
            
            data = getFromMinio(minioHost, minioPort, minioAccessKey, minioSecretKey, fileBucket, path)
            data = data.readlines()
            
            query = createDetailsQuery(data, trialID, experimentID, host)       
            query = sc.parallelize(query, sc.defaultParallelism * partitionsPerCore)
            query.saveToCassandra(cassandraKeyspace, "faban_details")
            
    
if __name__ == '__main__':
    main()