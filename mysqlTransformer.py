import sys

from pyspark_cassandra import CassandraSparkContext
from pyspark import SparkConf

# Hard coded for now
# This saves to a Cassandra database the values of ID and START_TIME_ from the CSV file.

conf = SparkConf() \
    .setAppName("PySpark Cassandra Test") \
    .setMaster(sys.argv[1]) \
    .set("spark.cassandra.connection.host", sys.argv[2])

sc = CassandraSparkContext(conf=conf)

#data = sc.textFile("/Users/Gabo/PycharmProjects/Test/Camunda_dump_example_csv.csv.gz")
data = sc.textFile(sys.argv[3])

count = data.map(lambda line: line.split(",")).map(lambda a: {"id" : a[0], "start_time" : a[5]}).saveToCassandra("test", "info")