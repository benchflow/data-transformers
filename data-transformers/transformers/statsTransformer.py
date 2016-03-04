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
#containerPropertiesID = sys.argv[7]
containerID = filePath.split("/")[-1].split("_")[0]
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

ioDataId = uuid.uuid1()

# Creates a dictionary
def createEDDict(a):
    ob = json.loads(a.decode())
    d = {}
    activeCpus = 0
    for c in ob["cpu_stats"]["cpu_usage"]["percpu_usage"]:
        if c != 0:
            activeCpus += 1
    if (ob["precpu_stats"]["cpu_usage"] != None) and ("total_usage" in ob["precpu_stats"]["cpu_usage"].keys()):
        cpu_percent = 0.0
        cpu_delta = ob["cpu_stats"]["cpu_usage"]["total_usage"] - ob["precpu_stats"]["cpu_usage"]["total_usage"]
        system_delta = ob["cpu_stats"]["system_cpu_usage"] - ob["precpu_stats"]["system_cpu_usage"]
        if system_delta > 0 and cpu_delta > 0:
            cpu_percent = 100.0 * (cpu_delta / float(system_delta * activeCpus))
        d["cpu_percent_usage"] = "%.2f" % (cpu_percent)
    d["environment_data_id"] = uuid.uuid1()
    d["trial_id"] = trialID
    d["experiment_id"] = experimentID
    d["container_id"] = containerID
    d["io_data_id"] = ioDataId
    d["read_time"] = ob["read"]
    d["cpu_total_usage"] = long(ob["cpu_stats"]["cpu_usage"]["total_usage"])
    d["cpu_percpu_usage"] = map(long, ob["cpu_stats"]["cpu_usage"]["percpu_usage"])
    d["memory_usage"] = long(ob["memory_stats"]["usage"]/1000000)
    d["memory_max_usage"] = long(ob["memory_stats"]["max_usage"]/1000000)
    d["cpu_throttling_data"] = ob["cpu_stats"]["throttling_data"]
    return d

# Calls Spark
query = data.map(createEDDict)
query.saveToCassandra(cassandraKeyspace, table, ttl=timedelta(hours=1))


IOData = json.loads(lines[-1].decode())
devices = {}
for dev in IOData["blkio_stats"]["io_service_bytes_recursive"]:
    devices["device"+str(dev["major"])] = {}

for dev in IOData["blkio_stats"]["io_service_bytes_recursive"]:
    devName = "device"+str(dev["major"])
    if "value" in dev.keys():
        if dev["op"] == "Read":
            devices[devName]["reads"] = dev["value"]
        if dev["op"] == "Write":
            devices[devName]["writes"] = dev["value"]
        if dev["op"] == "Sync":
            devices[devName]["sync"] = dev["value"]
        if dev["op"] == "Async":
            devices[devName]["async"] = dev["value"]
        if dev["op"] == "Total":
            devices[devName]["total"] = dev["value"]
    else:
        if dev["op"] == "Read":
            devices[devName]["reads"] = 0
        if dev["op"] == "Write":
            devices[devName]["writes"] = 0
        if dev["op"] == "Sync":
            devices[devName]["sync"] = 0
        if dev["op"] == "Async":
            devices[devName]["async"] = 0
        if dev["op"] == "Total":
            devices[devName]["total"] = 0

query = []
for dev in devices.keys():
    print(devices[dev])
    query.append({"experiment_id":experimentID, "trial_id":trialID, "io_data_id":ioDataId, "container_id":containerID, "device":dev, "reads":devices[dev]["reads"], \
        "writes":devices[dev]["writes"], "sync":devices[dev]["sync"], "async":devices[dev]["async"], "total":devices[dev]["total"]})

query = sc.parallelize(query)
query.saveToCassandra(cassandraKeyspace, "io_data", ttl=timedelta(hours=1))