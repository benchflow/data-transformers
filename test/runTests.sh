#!/bin/sh

echo "Starting Python tests"

python2.7 /test/python/statsTests.py
python2.7 /test/python/mysqlTests.py
python2.7 /test/python/propertiesTests.py

exit 0

echo "Starting Spark tests"

SPARK_MASTER=local[*]
PYSPARK_CASSANDRA=/pyspark-cassandra-assembly-0.3.5.jar
CASSANDRA_HOST=cassandra
MINIO_HOST="minio"
TRIAL_ID="camundaZZZZZZMV_ZZZZZZMV"
EXPERIMENT_ID="camundaZZZZZZMV"
SUT_NAME="camunda"
HOST_NAME="docker_host"

$SPARK_HOME/bin/spark-submit \
--master $SPARK_MASTER \
--jars $PYSPARK_CASSANDRA \
--driver-class-path $PYSPARK_CASSANDRA \
--conf spark.cassandra.connection.host=$CASSANDRA_HOST \
--files $TRANSFORMERS_PATH/conf/data-transformers/camunda.data-transformers.yml \
--py-files $PYSPARK_CASSANDRA,$TRANSFORMERS_PATH/commons/commons.py \
$TRANSFORMERS_PATH/transformers/statsTransformer.py \
'{"minio_host": "'$MINIO_HOST'", "file_path": "runs/mockStats/mockStats.gz", "sut_name": "'$SUT_NAME'", "trial_id": "'$TRIAL_ID'", "experiment_id": "'$EXPERIMENT_ID'", "container_id": "stats_camunda", "host_id": "'$HOST_NAME'"}'
if [ "$?" = "1" ]; then
	exit 1
fi

$SPARK_HOME/bin/spark-submit \
--master $SPARK_MASTER \
--jars $PYSPARK_CASSANDRA \
--driver-class-path $PYSPARK_CASSANDRA \
--conf spark.cassandra.connection.host=$CASSANDRA_HOST \
--files $TRANSFORMERS_PATH/conf/data-transformers/camunda.data-transformers.yml \
--py-files $PYSPARK_CASSANDRA,$TRANSFORMERS_PATH/commons/commons.py \
$TRANSFORMERS_PATH/transformers/mysqlTransformer.py \
'{"minio_host": "'$MINIO_HOST'", "file_path": "runs/mockProcessEngine", "sut_name": "'$SUT_NAME'", "trial_id": "'$TRIAL_ID'", "experiment_id": "'$EXPERIMENT_ID'", "container_id": "mysql_camunda", "host_id": "'$HOST_NAME'"}'
if [ "$?" = "1" ]; then
	exit 1
fi

for SCRIPT in "cassandraTest"
do 
	$SPARK_HOME/bin/spark-submit \
	--master $SPARK_MASTER \
	--jars $PYSPARK_CASSANDRA \
	--driver-class-path $PYSPARK_CASSANDRA \
	--conf spark.cassandra.connection.host=$CASSANDRA_HOST \
	--py-files $PYSPARK_CASSANDRA,$TRANSFORMERS_PATH/commons/commons.py \
	/test/spark/$SCRIPT.py
	if [ "$?" = "1" ]; then
		exit 1
	fi
done

exit 0