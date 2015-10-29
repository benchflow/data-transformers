import sys
import json

from pyspark_cassandra import CassandraSparkContext
from pyspark import SparkConf
from pyspark import SparkFiles

# Takes arguments: Spark master, Cassandra host, location of the CSV file

conf = SparkConf() \
    .setAppName("PySpark Cassandra Test") \
    .setMaster(sys.argv[1]) \
    .set("spark.cassandra.connection.host", sys.argv[2])

sc = CassandraSparkContext(conf=conf)

data = sc.textFile(sys.argv[3])

confPath = SparkFiles.get("conf.json")
with open(confPath) as f:
    conf = json.load(f)

indexes = {}
schema = data.first().split(",")

for i in range(len(schema)):
    for c in conf["column_mapping"]:
        if schema[i].replace('"', '') == c:
            indexes[schema[i].replace('"', '')] = i

print(schema)
print(indexes)

def createDic(a):
    d = {}
    for i in indexes:
        d[conf["column_mapping"][i]] = a[indexes[i]]
    return d

count = data.map(lambda line: line.split(",")).map(createDic).saveToCassandra("test", conf["dest_table"])