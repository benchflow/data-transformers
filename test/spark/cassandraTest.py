from pyspark_cassandra import CassandraSparkContext
from pyspark import SparkConf

def main():
    # Set configuration for spark context
    conf = SparkConf() \
        .setAppName("Test") \
        .setMaster("local[*]") \
        .set("spark.cassandra.connection.host", "cassandra")
    sc = CassandraSparkContext(conf=conf)
    
    dataIsEmpty = sc.cassandraTable("benchflow", "environment_data")\
            .select("trial_id") \
            .where("trial_id=? AND experiment_id=?", "camundaZZZZZZMV_ZZZZZZMV", "camundaZZZZZZMV") \
            .isEmpty()
            
    assert not dataIsEmpty, "Test failed"
    
    dataIsEmpty = sc.cassandraTable("benchflow", "process")\
            .select("trial_id") \
            .where("trial_id=? AND experiment_id=?", "camundaZZZZZZMV_ZZZZZZMV", "camundaZZZZZZMV") \
            .isEmpty()
            
    assert not dataIsEmpty, "Test failed"
    
    dataIsEmpty = sc.cassandraTable("benchflow", "database_sizes")\
            .select("trial_id") \
            .where("trial_id=? AND experiment_id=?", "camundaZZZZZZMV_ZZZZZZMV", "camundaZZZZZZMV") \
            .isEmpty()
            
    assert not dataIsEmpty, "Test failed"
    
    dataIsEmpty = sc.cassandraTable("benchflow", "io_data")\
            .select("trial_id") \
            .where("trial_id=? AND experiment_id=?", "camundaZZZZZZMV_ZZZZZZMV", "camundaZZZZZZMV") \
            .isEmpty()
            
    assert not dataIsEmpty, "Test failed"
    
    print("Cassandra tests passed")
    
if __name__ == '__main__': main()