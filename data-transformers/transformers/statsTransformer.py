import sys
import json
import urllib2
import io
import gzip
import uuid
import yaml

from datetime import timedelta

# Creates a dictionary
def createEDDict(a, trialID, experimentID, containerID, hostID, activeCpus):
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
    d["environment_data_id"] = uuid.uuid1().urn[9:]
    d["trial_id"] = trialID
    d["experiment_id"] = experimentID
    d["container_id"] = containerID
    d["host_id"] = hostID
    d["read_time"] = ob["read"]
    d["cpu_total_usage"] = long(ob["cpu_stats"]["cpu_usage"]["total_usage"])
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
    d["cpu_cores"] = activeCpus
    d["memory_usage"] = float(ob["memory_stats"]["usage"]/1000000.0)
    d["memory_max_usage"] = float(ob["memory_stats"]["max_usage"]/1000000.0)
    return d

def createIODict(a, trialID, experimentID, containerID, hostID):
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
        d["io_data_id"] = uuid.uuid1().urn[9:]
        d["trial_id"] = trialID
        d["experiment_id"] = experimentID
        d["container_id"] = containerID
        d["host_id"] = hostID
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

def getFromMinio(url):
    from commons import getFromUrl
    return getFromUrl(url)

def main():
    from pyspark_cassandra import CassandraSparkContext
    from pyspark import SparkConf
    from pyspark import SparkFiles
    
    # Takes arguments
    args = json.loads(sys.argv[1])
    minioHost = str(args["minio_host"])
    filePath = str(args["file_path"])
    trialID = str(args["trial_id"])
    experimentID = str(args["experiment_id"])
    containerID = str(args["container_id"])
    hostID = str(args["host_id"])
    
    statsTable = "environment_data"
    ioTable = "io_data"
    alluxioHost = "localhost"
    
    # Set configuration for spark context
    conf = SparkConf().setAppName("Stats Transformer")
    sc = CassandraSparkContext(conf=conf)
    
    # Retrieve the configuration file that was sent to spark
    #confPath = SparkFiles.get("data-transformers.yml")
    #with open(confPath) as f:
    #    transformerConfiguration = yaml.load(f)
    #    cassandraKeyspace = transformerConfiguration["cassandra_keyspace"]
    #    minioPort = transformerConfiguration["minio_port"]
    
    cassandraKeyspace = "benchflow"
    minioPort = "9000"
    
    #res = urllib2.urlopen("http://"+minioHost+":"+minioPort+"/"+filePath)
    #compressed = io.BytesIO(res.read())
    #decompressed = gzip.GzipFile(fileobj=compressed)
    #lines = decompressed.readlines()
    lines = getFromMinio("http://"+minioHost+":"+minioPort+"/"+filePath)
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
    
    # Calls Spark
    f = lambda a: createEDDict(a, trialID, experimentID, containerID, hostID, activeCpus)
    query = data.map(f)
    try: 
        query.saveAsTextFile("alluxio://"+alluxioHost+":19998/"+trialID+"_"+experimentID+"_"+containerID+"_"+hostID+"_environment_data")
    except:
        print("Could not save on alluxio file "+trialID+"_"+containerID+"_environment_data")
    query.saveToCassandra(cassandraKeyspace, statsTable, ttl=timedelta(hours=1))
    
    f = lambda a: createIODict(a, trialID, experimentID, containerID, hostID)
    query = data.map(f).reduce(lambda a, b: a+b)
    query = sc.parallelize(query)
    query.saveToCassandra(cassandraKeyspace, ioTable, ttl=timedelta(hours=1))
    
if __name__ == '__main__':
    main()