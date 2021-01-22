#!/bin/bash

GRAPH_FILE=$1
REVERSE_FILE=$2
INCREMENTAL_EXEC_PLANS_PATH=$3
UPDATES_EDGES_FILE=$4
BATCH=$5

ENUMERATE_PACKAGE=cn.edu.nju.pasa.graph.analysis.subgraph.dynamic
OUTPUT_FILE=null

echo "GRAPH_FILE: " $GRAPH_FILE
echo "REVERSE_FILE: " $REVERSE_FILE
echo "UPDATES_EDGES_FILE: " $UPDATES_EDGES_FILE
PARTITION_NUM=$((16 * 12 * 2 * 2))
echo "partition.num: " $PARTITION_NUM

hdfs dfs -rmr $OUTPUT_FILE/*

echo "----------START: " `date` "----------"

JAR=./spark-enumerator-opt4-allinone.jar
CLASS=cn.edu.nju.pasa.graph.analysis.subgraph.DynamicSubgraphEnumerationGeneric2
TASK_STAT_FILE=null

spark-submit --master yarn \
             --driver-memory 40g \
             --executor-memory 40g \
             --num-executors 16 \
             --executor-cores 2 \
             --conf spark.locality.wait=0s \
             --conf spark.sql.shuffle.partitions=$PARTITION_NUM \
             --conf spark.executor.extraJavaOptions="-XX:+PrintGCDetails -ea -XX:+PrintGCDateStamps" \
             --files pasa.conf.prop \
             --class $CLASS \
             $JAR \
             initial.forward.graph.path=$GRAPH_FILE \
             initial.reverse.graph.path=$REVERSE_FILE \
             update.edges.path=$UPDATES_EDGES_FILE \
             update.edges.batch=$BATCH \
             partition.num=$PARTITION_NUM \
             num.working.threads=12 \
             blocking.queue.size=1000 \
             output.path=$OUTPUT_FILE \
             incremental.execution.plans.path=$INCREMENTAL_EXEC_PLANS_PATH \
             enable.load.balance=false \
             enable.adaptive.balance.threshold=true \
             num.split.internode=32 \
             task.stats.output.path=$TASK_STAT_FILE
retcode=$?
echo "----------END: " `date` "----------"
echo "Exit value: " $retcode
