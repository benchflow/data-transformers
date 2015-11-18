import sys
import json
import urllib.request
import io
import gzip

from dataTransformations import Transformations

from pyspark_cassandra import CassandraSparkContext
from pyspark import SparkConf
from pyspark import SparkFiles

# Takes arguments: Spark master, Cassandra host, Minio host, path of the files
sparkMaster = sys.argv[1]
cassandraHost = sys.argv[2]
minioHost = sys.argv[3]
filePath = sys.argv[4]

conf = SparkConf() \
    .setAppName("PySpark Cassandra Test") \
    .setMaster(sparkMaster) \
    .set("spark.cassandra.connection.host", cassandraHost)

sc = CassandraSparkContext(conf=conf)

confPath = SparkFiles.get("conf.json")
with open(confPath) as f:
    conf = json.load(f)

res = urllib.request.urlopen("http://"+minioHost+"/"+filePath+"_"+conf["src_table"]+".gz")
compressed = io.BytesIO(res.read())
decompressed = gzip.GzipFile(fileobj=compressed)
lines = decompressed.readlines()
data = sc.parallelize(lines)

indexes = {}
print(data.first())
schema = data.first().decode().split(",")

for i in range(len(schema)):
    for c in conf["column_mapping"]:
        if schema[i].replace('"', '') == c:
            indexes[schema[i].replace('"', '')] = i

print(schema)
print(indexes)

def createDic(a):
    d = {}
    if(a[0] == schema[0]):
        return {"id":"0", "duration":"0"}
    for i in indexes:
        col = conf["column_mapping"][i]
        t = conf["column_transformation"].get(i)
        if(t == None):
            d[col] = a[indexes[i]]
        else:
            tf = getattr(Transformations, t)
            d[col] = tf(a[indexes[i]])
    return d

query = data.map(lambda line: line.decode().split(",")).map(createDic)
query.saveToCassandra("test", conf["dest_table"])