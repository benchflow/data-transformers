import sys
import json
import urllib2
import io
import gzip
import uuid
import yaml

from datetime import timedelta

#Function to create the Cassandra query for the container properties
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
    if "Image" in ob["Config"].keys():
        d["image"] = ob["Config"]["Image"]
    if "Labels" in ob["Config"].keys():
        labelsList = []
        for k in ob["Config"]["Labels"]:
            labelsList.append(k+":"+ob["Config"]["Labels"][k])
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
    if "LogConfig" in ob["HostConfig"].keys() and "Type" in ob["HostConfig"]["LogConfig"].keys():
        d["log_driver"] = ob["HostConfig"]["LogConfig"]["Type"]
    if "User" in ob["Config"].keys():
        d["user"] = ob["Config"]["User"]
    return d

#Function to create the Cassandra query for the host properties (the info data)
def createInfoDict(a):
    ob = json.loads(a.decode())
    d = {}
    if "ID" in ob.keys():
        d["host_id"] = ob["ID"]  
    if "CpuCfsPeriod" in ob.keys():
        d["cpu_cfs_period"] = ob["CpuCfsPeriod"]
    if "CpuCfsQuota" in ob.keys():
        d["cpu_cfs_quota"] = ob["CpuCfsQuota"]
    if "Debug" in ob.keys():
        d["debug"] = ob["Debug"]
    if "Driver" in ob.keys():
        d["driver"] = ob["Driver"]
    if "DockerRootDir" in ob.keys():
        d["docker_root_dir"] = ob["DockerRootDir"]
    if "ExecutionDriver" in ob.keys():
        d["execution_driver"] = ob["ExecutionDriver"]
    if "ExperimentalBuild" in ob.keys():
        d["experimental_build"] = ob["ExperimentalBuild"]
    if "HttpProxy" in ob.keys():
        d["http_proxy"] = ob["HttpProxy"]
    if "HttpsProxy" in ob.keys():
        d["https_proxy"] = ob["HttpsProxy"]
    if "IPv4Forwarding" in ob.keys():
        d["ipv4_forwarding"] = ob["IPv4Forwarding"]
    if "IndexServerAddress" in ob.keys():
        d["index_server_address"] = ob["IndexServerAddress"]
    if "KernelVersion" in ob.keys():
        d["kernel_version"] = ob["KernelVersion"]
    if "Labels" in ob.keys():
        d["labels"] = ob["Labels"]
    if "MemoryLimit" in ob.keys():
        d["memory_limit"] = ob["MemoryLimit"]
    if "MemTotal" in ob.keys():
        d["mem_total"] = ob["MemTotal"]
    if "NCPU" in ob.keys():
        d["n_cpu"] = ob["NCPU"]
    if "NEventsListener" in ob.keys():
        d["n_events_listener"] = ob["NEventsListener"]
    if "NFd" in ob.keys():
        d["n_fd"] = ob["NFd"]
    if "NGoroutines" in ob.keys():
        d["n_goroutines"] = ob["NGoroutines"]
    if "Name" in ob.keys():
        d["name"] = ob["Name"]
    if "NoProxy" in ob.keys():
        d["no_proxy"] = ob["NoProxy"]
    if "OomKillDisable" in ob.keys():
        d["oom_kill_disable"] = ob["OomKillDisable"]
    if "OperatingSystem" in ob.keys():
        d["operating_system"] = ob["OperatingSystem"]
    if "SwapLimit" in ob.keys():
        d["swap_limit"] = ob["SwapLimit"]
    if "SystemTime" in ob.keys():
        d["system_time"] = ob["SystemTime"]
    if "ServerVersion" in ob.keys():
        d["server_version"] = ob["ServerVersion"]
    return d

#Function to create the Cassandra query for the Docker version data
def createVersionDict(a):
    ob = json.loads(a.decode())
    d = {}
    for data in ob:
        data = data.split("=")
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

#Function to get the raw data from Minio
def getFromMinio(minioHost, minioPort, accessKey, secretKey, bucket, path):
    from commons import getFromMinio
    return getFromMinio(minioHost, minioPort, accessKey, secretKey, bucket, path)

#Function to check if the host data needs to be saved again, checking if the host data are already on Cassandra
def hostNeedsSaving(sc, infoData, cassandraKeyspace):
    ob = json.loads(infoData.decode())
    if "ID" in ob.keys():
        hostID = ob["ID"]
    else:
        return false
    
    #for e in ob:
    #    e = e.split("=")
    #    if e[0] == "ID":
    #        hostID = e[1]
    #        break
    
    #if hostID == "":
    #    return false
    
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
    
    #Get the container properties data
    inspectData = getFromMinio(minioHost, minioPort, minioAccessKey, minioSecretKey, fileBucket, filePath+"_inspect.gz").readlines()[0] 
    
    #Transform container properties and save to Cassandra
    query = createContainerDict(inspectData, trialID, experimentID, containerID, hostID)
    query = sc.parallelize([query])
    query.saveToCassandra(cassandraKeyspace, "container_properties")
    
    #Get the host properties data
    infoData = getFromMinio(minioHost, minioPort, minioAccessKey, minioSecretKey, fileBucket, filePath+"_info.gz").readlines()[0]
    #Get the Docker version data
    versionData = getFromMinio(minioHost, minioPort, minioAccessKey, minioSecretKey, fileBucket, filePath+"_version.gz").readlines()[0]
    #Check if we need to save the host properties
    hostNotSaved = hostNeedsSaving(sc, infoData, cassandraKeyspace)
    #If host needs to be saved, transform and save data
    if hostNotSaved:
        query = createInfoDict(infoData)
        query.update(createVersionDict(versionData))
        query = sc.parallelize([query])
        query.saveToCassandra(cassandraKeyspace, "host_properties")
    
if __name__ == '__main__':
    main()