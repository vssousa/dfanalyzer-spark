#!/bin/bash
rm last_data_generation.log
rm -rf *.txt
CUSTOMERS=$1
CLOTH_ITEMS=$2
BUYING_PATTERNS=$3
java -jar bin/Clothing-DataSetGenerator-1.0.jar $CUSTOMERS $CLOTH_ITEMS $BUYING_PATTERNS
echo "java -jar bin/Clothing-DataSetGenerator-1.0.jar $CUSTOMERS $CLOTH_ITEMS $BUYING_PATTERNS" >> last_data_generation.log
cat last_data_generation.log