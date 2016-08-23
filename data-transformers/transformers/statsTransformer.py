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
    d["memory_usage"] = float(ob["memory_stats"]["usage"]/(1024*1024))
    d["memory_max_usage"] = float(ob["memory_stats"]["max_usage"]/(1024*1024))
    return d

def createNetworkDict(a, trialID, experimentID, containerID, hostID):
    ob = json.loads(a.decode())
    dicts = []
    for n in ob["networks"]:
        d = {}
        d["network_interface_data_id"] = uuid.uuid1().urn[9:]
        d["trial_id"] = trialID
        d["experiment_id"] = experimentID
        d["container_id"] = containerID
        d["host_id"] = hostID
        d["network_interface_name"] = n
        d["network_rx_bytes"] = ob["networks"][n]["rx_bytes"]
        d["network_tx_bytes"] = ob["networks"][n]["tx_bytes"]
        d["network_rx_packets"] = ob["networks"][n]["rx_packets"]
        d["network_tx_packets"] = ob["networks"][n]["tx_packets"]
        dicts.append(d)
    return dicts

def createHostNetworkDict(a, trialID, experimentID, containerID, hostID):
    d = {}
    d["network_interface_data_id"] = uuid.uuid1().urn[9:]
    d["trial_id"] = trialID
    d["experiment_id"] = experimentID
    d["container_id"] = containerID
    d["host_id"] = hostID
    d["network_interface_name"] = "host"
    d["network_tx_bytes"] = a[0]
    d["network_rx_bytes"] = a[1]
    return d

def createIODict(a, trialID, experimentID, containerID, hostID):
    ob = json.loads(a.decode())
    dicts = []
    dd = {}
    if not "io_service_bytes_recursive" in ob["blkio_stats"]:
        d = {}
        d["io_data_id"] = uuid.uuid1().urn[9:]
        d["trial_id"] = trialID
        d["experiment_id"] = experimentID
        d["container_id"] = containerID
        d["host_id"] = hostID
        dicts.append(d)
        return dicts
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

def getBytesSumForContainerPIDS(e, PIDS):
    e = filter((lambda a: not "Refreshing" in a), e)
    e = map((lambda a: a.strip("\n").split("\t")), e)
    e = filter((lambda a: len(a) == 3 and a[0].split("/")[-2] in PIDS), e)
    if len(e) == 0:
        return (0, 0)
    e = map((lambda a: (float(a[1])*1000,float(a[2])*1000)), e)
    e = reduce((lambda a, b: (a[0]+b[0],a[1]+b[1])), e)
    return e

def createNetworkHostQuery(sc, top, net, trialID, experimentID, containerID, hostID):
    def getPid(a):
        data = json.loads(a.decode())
        pids = []
        for p in data["Processes"]:
            pids.append(p[1])
        return pids
    PIDS = sc.parallelize(top).map(getPid).reduce(lambda a, b: a+b)
    
    netPerSecond = []
    chunk = []
    for line in net[1:]:
        if "Refreshing" in line:
            netPerSecond.append(chunk)
            chunk = []
        else:
            chunk.append(line)

    data = sc.parallelize(netPerSecond).map(lambda a: getBytesSumForContainerPIDS(a, PIDS)).collect()
    i = 0
    for i in range(len(data)-1):
        data[i+1] = (data[i+1][0]+data[i][0], data[i+1][0]+data[i][1])
    
    f = lambda a: createHostNetworkDict(a, trialID, experimentID, containerID, hostID)
    query = sc.parallelize(data).map(f).collect()
    return query

def getFromMinio(minioHost, minioPort, accessKey, secretKey, bucket, path):
    from commons import getFromMinio
    return getFromMinio(minioHost, minioPort, accessKey, secretKey, bucket, path)

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
    containerID = str(args["container_id"])
    hostID = str(args["host_id"])
    partitionsPerCore = 5
    
    statsTable = "environment_data"
    ioTable = "io_data"
    alluxioHost = "localhost"
    
    # Set configuration for spark context
    conf = SparkConf().setAppName("Stats Transformer")
    sc = CassandraSparkContext(conf=conf)
    
    lines = getFromMinio(minioHost, minioPort, minioAccessKey, minioSecretKey, fileBucket, filePath+"_stats.gz").readlines()
    data = sc.parallelize(lines, sc.defaultParallelism * partitionsPerCore)
    
    activeCpus = 0
    for l in lines:
        acpus = 0
        ob = json.loads(l.decode())
        for c in ob["cpu_stats"]["cpu_usage"]["percpu_usage"]:
            if c != 0:
                acpus += 1
        if acpus > activeCpus:
            activeCpus = acpus
    
    # Saving Stats data
    ####################
    f = lambda a: createEDDict(a, trialID, experimentID, containerID, hostID, activeCpus)
    query = data.map(f)
    query.saveToCassandra(cassandraKeyspace, statsTable)
    
    
    # Saving IO Data
    ####################
    f = lambda a: createIODict(a, trialID, experimentID, containerID, hostID)
    query = data.map(f).reduce(lambda a, b: a+b)
    query = sc.parallelize(query, sc.defaultParallelism * partitionsPerCore)
    query.saveToCassandra(cassandraKeyspace, ioTable)
    
    
    #Saving Network data
    ####################
    firstStats = json.loads(lines[0].decode())
    if "networks" in firstStats.keys() and len(firstStats) != 0:
        networkDataAvailable = True
    else:
        networkDataAvailable = False
    
    if networkDataAvailable:
        f = lambda a: createNetworkDict(a, trialID, experimentID, containerID, hostID)
        query = data.map(f).reduce(lambda a, b: a+b)
    else: 
        net = getFromMinio(minioHost, minioPort, minioAccessKey, minioSecretKey, fileBucket, filePath+"_network.gz")
        net = net.readlines()
        
        top = getFromMinio(minioHost, minioPort, minioAccessKey, minioSecretKey, fileBucket, filePath+"_top.gz")
        top = top.readlines()
        
        query = createNetworkHostQuery(sc, top, net, trialID, experimentID, containerID, hostID)
        
    sc.parallelize(query).saveToCassandra(cassandraKeyspace, "network_interface_data")
    
if __name__ == '__main__':
    main()