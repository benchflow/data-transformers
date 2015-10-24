import sys

from pyspark_cassandra import CassandraSparkContext
from pyspark import SparkConf

# Takes arguments: Spark master, Cassandra host, location of the CSV file

# Hard coded for now
# This saves to a Cassandra database the values of ID and START_TIME_ from the CSV file.
columns = ["ID_", "START_TIME_"]

conf = SparkConf() \
    .setAppName("PySpark Cassandra Test") \
    .setMaster(sys.argv[1]) \
    .set("spark.cassandra.connection.host", sys.argv[2])

sc = CassandraSparkContext(conf=conf)

#data = sc.textFile("/Users/Gabo/PycharmProjects/Test/Camunda_dump_example_csv.csv.gz")
data = sc.textFile(sys.argv[3])

indexes = {}
schema = data.first().split(",")

for i in range(len(schema)):
    for c in columns:
        if schema[i].replace('"', '') == c:
            indexes[schema[i].replace('"', '').lower()] = i

def createDic(a):
    d = {}
    for i in indexes:
        d[i] = a[indexes[i]]
    return d

count = data.map(lambda line: line.split(",")).map(createDic).saveToCassandra("test", "info")