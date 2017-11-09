#!/bin/bash
clear
echo "Setting up environment variables"
DATAFLOW_TAG=clothing
PROPERTY_FILE=property.config
SIMULATION_DIR=`pwd`
echo "-------------------------------------------------"
echo "Removing files from previous executions"
rm spark-initialization.log
rm $PROPERTY_FILE
echo "-------------------------------------------------"
echo "Configuring property file for Spark application"
echo "DATAFLOW_TAG="$DATAFLOW_TAG >> $PROPERTY_FILE
echo "MASTER="$SPARK_MASTER >> $PROPERTY_FILE
echo "WORKSPACE="$SIMULATION_DIR >> $PROPERTY_FILE
echo "DEBUG=true" >> $PROPERTY_FILE
echo "FASTBIT_PATH="$FASTBIT_BIN >> $PROPERTY_FILE
echo "RESTFUL=localhost" >> $PROPERTY_FILE
echo "-------------------------------------------------"
echo "Configuring Spark"
cp spark-env/slaves spark-env/spark-env.sh $SPARK_HOME/conf
echo "-------------------------------------------------"
echo "Stopping Spark master and workers"
$SPARK_HOME/sbin/stop-all.sh
echo "-------------------------------------------------"
echo "Starting Spark master and workers"
$SPARK_SBIN/start-all.sh >> spark-initialization.log
sleep 5
export SPARK_MASTER=`python bin/MasterURLDiscovery.py spark-initialization.log`
echo "-------------------------------------------------"
echo "Submitting dataflow specification"
$SIMULATION_DIR/bin/send-dataflow-spec.sh
sleep 3
echo "-------------------------------------------------"
echo "Submitting a Spark application" 
$SPARK_HOME/bin/spark-submit \
	--class dataflow.ClothingApplication  \
	--master local[1] \
	bin/Clothing-Spark-App-1.0.jar 
echo "-------------------------------------------------"
