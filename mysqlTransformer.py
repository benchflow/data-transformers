import sys
import json
import urllib.request
import io
import gzip

from dataTransformations import Transformations

from pyspark_cassandra import CassandraSparkContext
from pyspark import SparkConf
from pyspark import SparkFiles

# Takes arguments: Spark master, Cassandra host, location of the CSV file

conf = SparkConf() \
    .setAppName("PySpark Cassandra Test") \
    .setMaster(sys.argv[1]) \
    .set("spark.cassandra.connection.host", sys.argv[2])

sc = CassandraSparkContext(conf=conf)

#data = sc.textFile(sys.argv[3])
res = urllib.request.urlopen("http://localhost:9000/test/Camunda_dump_example_csv.csv.gz")
compressed = io.BytesIO(res.read())
decompressed = gzip.GzipFile(fileobj=compressed)
lines = decompressed.readlines()
data = sc.parallelize(lines)

confPath = SparkFiles.get("conf.json")
with open(confPath) as f:
    conf = json.load(f)

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