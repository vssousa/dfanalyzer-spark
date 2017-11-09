#!/bin/bash
echo "-------------------------------------------------"
echo "Removing data from previous executions"
# organizing provenance directories
rm -rf provenance/*
rm DfA.properties
# cleaning up MonetDB directory
rm -rf data
rm prov-db.dump
# cleaning up Spark's environment
rm spark-master.conf
rm spark-slaves.conf
rm -rf output/*
rm *.log
rm property.config
# configuring computational environment for application run
rm database.conf
rm nodes.txt