#!/bin/bash
cd input_dataset
CUSTOMERS=$1
CLOTH_ITEMS=$2
BUYING_PATTERNS=$3
./generate_dataset.sh $CUSTOMERS $CLOTH_ITEMS $BUYING_PATTERNS
ls -l
cd ..