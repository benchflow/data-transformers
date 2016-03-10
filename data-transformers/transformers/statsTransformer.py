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

activeCpus = 0
for l in lines:
    acpus = 0
    ob = json.loads(l.decode())
    for c in ob["cpu_stats"]["cpu_usage"]["percpu_usage"]:
        if c != 0:
            acpus += 1
    if acpus > activeCpus:
        activeCpus = acpus

# Creates a dictionary
def createEDDict(a):
    ob = json.loads(a.decode())
    d = {}
    if (ob["precpu_stats"]["cpu_usage"] is not None) and ("total_usage" in ob["precpu_stats"]["cpu_usage"].keys()):
        cpu_percent = 0.0
        cpu_delta = float(ob["cpu_stats"]["cpu_usage"]["total_usage"]) - float(ob["precpu_stats"]["cpu_usage"]["total_usage"])
        system_delta = float(ob["cpu_stats"]["system_cpu_usage"]) - float(ob["precpu_stats"]["system_cpu_usage"])
        if system_delta > 0.0 and cpu_delta > 0.0:
            cpu_percent = (cpu_delta / system_delta) * float(len(ob["cpu_stats"]["cpu_usage"]["percpu_usage"])) * 100.0
            cpu_percent = cpu_percent/activeCpus
        d["cpu_percent_usage"] = cpu_percent
    d["environment_data_id"] = uuid.uuid1()
    d["trial_id"] = trialID
    d["experiment_id"] = experimentID
    d["container_id"] = containerID
    d["read_time"] = ob["read"]
    d["cpu_total_usage"] = long(ob["cpu_stats"]["cpu_usage"]["total_usage"])
    #d["cpu_percpu_usage"] = map(long, ob["cpu_stats"]["cpu_usage"]["percpu_usage"])
    perCpuUsages = []
    if (ob["precpu_stats"]["cpu_usage"] is not None) and ("percpu_usage" in ob["precpu_stats"]["cpu_usage"].keys()):
        for i in range(len(ob["cpu_stats"]["cpu_usage"]["percpu_usage"])):
            cpu_percent = 0.0
            cpu_delta = float(ob["cpu_stats"]["cpu_usage"]["percpu_usage"][i]) - float(ob["precpu_stats"]["cpu_usage"]["percpu_usage"][i])
            system_delta = float(ob["cpu_stats"]["system_cpu_usage"]) - float(ob["precpu_stats"]["system_cpu_usage"])
            if system_delta > 0.0 and cpu_delta > 0.0:
                cpu_percent = (cpu_delta / system_delta) * float(len(ob["cpu_stats"]["cpu_usage"]["percpu_usage"])) * 100.0
            perCpuUsages.append(cpu_percent)
    d["cpu_percpu_percent_usage"] = perCpuUsages
    d["cpu_percpu_usage"] = map(long, ob["cpu_stats"]["cpu_usage"]["percpu_usage"])
    d["cpu_cores"] = len(ob["cpu_stats"]["cpu_usage"]["percpu_usage"])
    d["memory_usage"] = float(ob["memory_stats"]["usage"]/1000000.0)
    d["memory_max_usage"] = float(ob["memory_stats"]["max_usage"]/1000000.0)
    return d

def createIODict(a):
    ob = json.loads(a.decode())
    dicts = []
    dd = {}
    for dev in ob["blkio_stats"]["io_service_bytes_recursive"]:
        devName = ""
        if "major" in dev.keys():
            devName = devName + str(dev["major"])
        if "minor" in dev.keys():
            devName = devName + "_" + str(dev["minor"])
        dd[devName] = {}
    for dev in ob["blkio_stats"]["io_service_bytes_recursive"]:
        if "value" in dev.keys():
            if dev["op"] == "Read":
                dd[devName]["reads"] = dev["value"]
            if dev["op"] == "Write":
                dd[devName]["writes"] = dev["value"]
            if dev["op"] == "Sync":
                dd[devName]["sync"] = dev["value"]
            if dev["op"] == "Async":
                dd[devName]["async"] = dev["value"]
            if dev["op"] == "Total":
                dd[devName]["total"] = dev["value"]
    for k in dd.keys():
        d = {}
        d["io_data_id"] = uuid.uuid1()
        d["trial_id"] = trialID
        d["experiment_id"] = experimentID
        d["container_id"] = containerID
        d["device"] = k
        if "reads" in dd[k].keys():
            d["reads"] = dd[k]["reads"]
        else:
            d["reads"] = None
        if "writes" in dd[k].keys():
            d["writes"] = dd[k]["writes"]
        else:
            d["writes"] = None
        if "sync" in dd[k].keys():
            d["sync"] = dd[k]["sync"]
        else:
            d["sync"] = None
        if "async" in dd[k].keys():
            d["async"] = dd[k]["async"]
        else:
            d["async"] = None
        if "total" in dd[k].keys():
            d["total"] = dd[k]["total"]
        else:
            d["total"] = None
        dicts.append(d)
    return dicts


# Calls Spark
query = data.map(createEDDict)
query.saveToCassandra(cassandraKeyspace, "environment_data", ttl=timedelta(hours=1))

query = data.map(createIODict).reduce(lambda a, b: a+b)
query = sc.parallelize(query)
query.saveToCassandra(cassandraKeyspace, "io_data", ttl=timedelta(hours=1))