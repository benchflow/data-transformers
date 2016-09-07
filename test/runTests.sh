#!/bin/sh

echo "Starting Python tests"

/usr/bin/python2.7 /test/python/statsTests.py
/usr/bin/python2.7 /test/python/mysqlTests.py
/usr/bin/python2.7 /test/python/propertiesTests.py

echo "Starting Spark tests"

PYSPARK_PYTHON=/usr/bin/python2.7
SPARK_MASTER=local[*]
PYSPARK_CASSANDRA=/pyspark-cassandra-assembly-0.3.5.jar
SUT_TEST_CONFIGURATION_FILE=/test/sut/wfms/camunda/3.5.0-3.7.0/data-transformers.configuration.yml
CASSANDRA_HOST=cassandra
MINIO_HOST="minio"
MINIO_PORT="9000"
TRIAL_ID="camundaZZZZZZMV_ZZZZZZMV"
EXPERIMENT_ID="camundaZZZZZZMV"
SUT_NAME="camunda"
HOST_NAME="docker_host"
CONFIG_FILE="data-transformers.configuration.yml"

$SPARK_HOME/bin/spark-submit \
--master $SPARK_MASTER \
--jars $PYSPARK_CASSANDRA \
--driver-class-path $PYSPARK_CASSANDRA \
--conf spark.cassandra.connection.host=$CASSANDRA_HOST \
--files $SUT_TEST_CONFIGURATION_FILE \
--py-files $PYSPARK_CASSANDRA,$TRANSFORMERS_PATH/commons/commons.py \
$TRANSFORMERS_PATH/transformers/statsTransformer.py \
'{"cassandra_keyspace":"benchflow", "minio_host": "'$MINIO_HOST'", "minio_port":"'$MINIO_PORT'", "minio_access_key":"AKIAIOSFODNN7EXAMPLE", "minio_secret_key":"wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY", "file_bucket":"runs", "file_path": "mockStats/mock", "sut_name": "'$SUT_NAME'", "trial_id": "'$TRIAL_ID'", "experiment_id": "'$EXPERIMENT_ID'", "container_id": "stats_camunda", "container_name": "stats_camunda", "host_id": "'$HOST_NAME'"}'
if [ "$?" = "1" ]; then
	exit 1
fi
sleep 5

$SPARK_HOME/bin/spark-submit \
--master $SPARK_MASTER \
--jars $PYSPARK_CASSANDRA \
--driver-class-path $PYSPARK_CASSANDRA \
--conf spark.cassandra.connection.host=$CASSANDRA_HOST \
--files $SUT_TEST_CONFIGURATION_FILE \
--py-files $PYSPARK_CASSANDRA,$TRANSFORMERS_PATH/commons/commons.py \
$TRANSFORMERS_PATH/transformers/mysqlTransformer.py \
'{"cassandra_keyspace":"benchflow", "minio_host": "'$MINIO_HOST'", "minio_port":"'$MINIO_PORT'", "minio_access_key":"AKIAIOSFODNN7EXAMPLE", "minio_secret_key":"wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY", "file_bucket":"runs", "file_path": "mockProcessEngine", "config_file": "'$CONFIG_FILE'", "trial_id": "'$TRIAL_ID'", "experiment_id": "'$EXPERIMENT_ID'", "container_id": "mysql_camunda", "container_name": "mysql_camunda", "host_id": "'$HOST_NAME'"}'
if [ "$?" = "1" ]; then
	exit 1
fi
sleep 5

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
sleep 5

exit 0