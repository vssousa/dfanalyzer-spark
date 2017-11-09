#!/bin/sh
#set -x
conf=$1
cpath=$2
datapath=$3
lines=`cat $conf  | egrep -v "#"`
dir=`pwd`

for i in `echo $lines`; do 
  host=`echo $i`
  # echo "cd $cpath;monetdbd start $datapath;monetdbd get all $datapath;monetdb start dataflow_analyzer;monetdb status"
  cd $cpath;monetdbd start $datapath;monetdbd get all $datapath;monetdb start dataflow_analyzer;monetdb status
  # echo "cd $cpath;killall monetdb;killall monetdbd;killall mserver5;monetdbd set port=54321 $datapath; monetdbd start $datapath;monetdbd get all $datapath;monetdb start dataflow_analyzer;monetdb status"
  # ssh $host "cd $cpath;monetdbd stop $datapath;killall monetdb;killall monetdbd;killall mserver5;monetdbd set port=54321 $datapath; monetdbd start $datapath;monetdbd get all $datapath;monetdb start dataflow_analyzer;monetdb status;" &
done