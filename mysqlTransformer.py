import sys

from pyspark_cassandra import CassandraSparkContext
from pyspark import SparkConf

conf = SparkConf() \
    .setAppName("PySpark Cassandra Test") \
    .setMaster(sys.argv[1]) \
    .set("spark.cassandra.connection.host", sys.argv[2])

sc = CassandraSparkContext(conf=conf)

data = sc.textFile("/Users/Gabo/PycharmProjects/Test/Camunda_dump_example_csv.csv")

count = data.map(lambda line: line.split(",")).map(lambda a: {"id" : a[0], "name" : a[1]}).saveToCassandra("test", "info")