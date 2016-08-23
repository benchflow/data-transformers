import sys
import json
import urllib2
import io
import gzip
import uuid
import yaml

from datetime import timedelta

# Creates a dictionary
def createContainerDict(a, trialID, experimentID, containerID, hostID):
    ob = json.loads(a.decode())
    d = {}
    d["container_properties_id"] = uuid.uuid1()
    d["trial_id"] = trialID
    d["experiment_id"] = experimentID
    d["container_id"] = containerID
    d["host_id"] = hostID
    if "Env" in ob["Config"].keys():
        d["environment"] = ob["Config"]["Env"]
    if "Image" in ob.keys():
        d["image"] = ob["Image"]
    if "Labels" in ob.keys():
        labelsList = []
        for k in ob["Labels"]:
            labelsList.append(k+":"+ob["Labels"][k])
        d["labels"] = labelsList
    if "Ulimits" in ob["HostConfig"].keys():
        ul = {}
        for lim in ob["HostConfig"]["Ulimits"]:
            if "Name" in lim and "Hard" in lim:
                ul[lim["Name"]] = lim["Hard"]
        d["u_limits"] = ul
    if "Links" in ob["HostConfig"].keys():
        d["links"] = ob["HostConfig"]["Links"]
    if "VolumeDriver" in ob["Config"].keys():
        d["volume_driver"] = ob["Config"]["VolumeDriver"]
    if "VolumesFroms" in ob["Config"].keys():
        d["volumes_from"] = ob["Config"]["VolumesFroms"]
    if "CpuShares" in ob["Config"].keys():
        d["cpu_shares"] = ob["Config"]["CpuShares"]
    if "CpusetCpus" in ob["HostConfig"].keys():
        d["cpu_set_cpus"] = ob["HostConfig"]["CpusetCpus"]
    if "CpusetMems" in ob["HostConfig"].keys():
        d["cpu_set_mems"] = ob["HostConfig"]["CpusetMems"]
    if "CpuQuota" in ob["HostConfig"].keys():
        d["cpu_quota"] = ob["HostConfig"]["CpuQuota"]
    if "CpuPeriod" in ob["HostConfig"].keys():
        d["cpu_period"] = ob["HostConfig"]["CpuPeriod"]
    if "BlkioWeight" in ob["HostConfig"].keys():
        d["blkio_weight"] = ob["HostConfig"]["BlkioWeight"]
    if "Memory" in ob["Config"].keys():
        d["mem_limit"] = ob["Config"]["Memory"]
    if "MemorySwap" in ob["Config"].keys():
        d["mem_swap_limit"] = ob["Config"]["MemorySwap"]
    if "MemorySwappiness" in ob["HostConfig"].keys():
        d["memory_swappiness"] = ob["HostConfig"]["MemorySwappiness"]
    if "OomKillDisable" in ob["HostConfig"].keys():
        d["oom_kill_disable"] = ob["HostConfig"]["OomKillDisable"]
    if "Privileged" in ob["HostConfig"].keys():
        d["privileged"] = ob["HostConfig"]["Privileged"]
    if "ReadonlyRootfs" in ob["HostConfig"].keys():
        d["read_only"] = ob["HostConfig"]["ReadonlyRootfs"]
    if "RestartPolicy" in ob["HostConfig"].keys() and "Name" in ob["HostConfig"]["RestartPolicy"]:
        d["restart_policy"] = ob["HostConfig"]["RestartPolicy"]["Name"]
    if "Name" in ob.keys():
        d["name"] = ob["Name"].replace("/", "")
    if "Driver" in ob.keys():
        d["log_driver"] = ob["Driver"]
    if "User" in ob["Config"].keys():
        d["user"] = ob["Config"]["User"]
    return d

def createInfoDict(a):
    ob = json.loads(a.decode())
    d = {}
    for data in ob:
        data = data.split("=")
        if data[0] == "ID":
            d["host_id"] = data[1]
            continue
        elif data[0] == "CpuCfsPeriod":
            d["cpu_cfs_period"] = data[1]
            continue
        elif data[0] == "CpuCfsQuota":
            d["cpu_cfs_quota"] = data[1]
            continue
        elif data[0] == "Debug":
            d["debug"] = data[1]
            continue
        elif data[0] == "Driver":
            d["driver"] = data[1]
            continue
        elif data[0] == "DockerRootDir":
            d["docker_root_dir"] = data[1]
            continue
        elif data[0] == "ExecutionDriver":
            d["execution_driver"] = data[1]
            continue
        elif data[0] == "ExperimentalBuild":
            d["experimental_build"] = data[1]
            continue
        elif data[0] == "HttpProxy":
            d["http_proxy"] = data[1]
            continue
        elif data[0] == "HttpsProxy":
            d["https_proxy"] = data[1]
            continue
        elif data[0] == "IPv4Forwarding":
            d["ipv4_forwarding"] = data[1]
            continue
        elif data[0] == "IndexServerAddress":
            d["index_server_address"] = data[1]
            continue
        elif data[0] == "KernelVersion":
            d["kernel_version"] = data[1]
            continue
        elif data[0] == "Labels":
            d["labels"] = data[1]
            continue
        elif data[0] == "MemoryLimit":
            d["memory_limit"] = data[1]
            continue
        elif data[0] == "MemTotal":
            d["mem_total"] = data[1]
            continue
        elif data[0] == "NCPU":
            d["n_cpu"] = data[1]
            continue
        elif data[0] == "NEventsListener":
            d["n_events_listener"] = data[1]
            continue
        elif data[0] == "NFd":
            d["n_fd"] = data[1]
            continue
        elif data[0] == "NGoroutines":
            d["n_goroutines"] = data[1]
            continue
        elif data[0] == "Name":
            d["name"] = data[1]
            continue
        elif data[0] == "NoProxy":
            d["no_proxy"] = data[1]
            continue
        elif data[0] == "OomKillDisable":
            d["oom_kill_disable"] = data[1]
            continue
        elif data[0] == "OperatingSystem":
            d["operating_system"] = data[1]
            continue
        elif data[0] == "SwapLimit":
            d["swap_limit"] = data[1]
            continue
        elif data[0] == "SystemTime":
            d["system_time"] = data[1]
            continue
        elif data[0] == "ServerVersion":
            d["server_version"] = data[1]
            continue
        else:
            continue
    return d

def createVersionDict(a):
    ob = json.loads(a.decode())
    d = {}
    for data in ob:
        data = data.split("=")
        print data
        if data[0] == "Version":
            d["docker_version"] = data[1]
            continue
        if data[0] == "Os":
            d["docker_os"] = data[1]
            continue
        if data[0] == "ApiVersion":
            d["docker_api_version"] = data[1]
            continue
        if data[0] == "GitCommit":
            d["docker_git_commit"] = data[1]
            continue
        if data[0] == "GoVersion":
            d["docker_go_version"] = data[1]
            continue
        if data[0] == "Arch":
            d["docker_arch"] = data[1]
            continue
        if data[0] == "KernelVersion":
            d["docker_kernel_version"] = data[1]
            continue
    return d


def getFromMinio(minioHost, minioPort, accessKey, secretKey, bucket, path):
    from commons import getFromMinio
    return getFromMinio(minioHost, minioPort, accessKey, secretKey, bucket, path)

def hostNeedsSaving(sc, infoData, cassandraKeyspace):
    hostID = ""
    
    ob = json.loads(infoData.decode())
    for e in ob:
        e = e.split("=")
        if e[0] == "ID":
            hostID = e[1]
            break
    
    if hostID == "":
        return false
    
    hostNotSaved = sc.cassandraTable(cassandraKeyspace, "host_properties") \
        .select("host_id") \
        .where("host_id=?", hostID) \
        .isEmpty()
        
    return hostNotSaved

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
    
    # Set configuration for spark context
    conf = SparkConf().setAppName("Properties Transformer")
    sc = CassandraSparkContext(conf=conf)
     
    inspectData = getFromMinio(minioHost, minioPort, minioAccessKey, minioSecretKey, fileBucket, filePath+"_inspect.gz").readlines()[0] 
    
    query = createContainerDict(inspectData, trialID, experimentID, containerID, hostID)
    query = sc.parallelize([query])
    query.saveToCassandra(cassandraKeyspace, "container_properties")
    
    infoData = getFromMinio(minioHost, minioPort, minioAccessKey, minioSecretKey, fileBucket, filePath+"_info.gz").readlines()[0]
    
    versionData = getFromMinio(minioHost, minioPort, minioAccessKey, minioSecretKey, fileBucket, filePath+"_version.gz").readlines()[0]
    
    hostNotSaved = hostNeedsSaving(sc, infoData, cassandraKeyspace)
        
    if hostNotSaved:
        query = createInfoDict(infoData)
        query.update(createVersionDict(versionData))
        query = sc.parallelize([query])
        query.saveToCassandra(cassandraKeyspace, "host_properties")
    
if __name__ == '__main__':
    main()