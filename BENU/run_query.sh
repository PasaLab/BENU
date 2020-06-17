#!/bin/bash

GRAPH_FILE=$1
#GRAPH_FILE=wangshen/as-skitter/resort
PATTERN=$2
MEMORY_LIMIT_IN_GB=40
THREAD_NUM=24
EXECUTORS_NUM=16
timeout=2h

OUTPUT_FILE=null

ENUMERATE_PACKAGE=cn.edu.nju.pasa.graph.analysis.subgraph.enumerator

case $PATTERN in
    triangle)
        ENUMERATE_CLASS=${ENUMERATE_PACKAGE}.Clique3Enumerator
        ;;
    clique4)
        ENUMERATE_CLASS=${ENUMERATE_PACKAGE}.Clique4Enumerator
        ;;
    clique5)
        ENUMERATE_CLASS=${ENUMERATE_PACKAGE}.Clique5Enumerator
        ;;
    house)
        ENUMERATE_CLASS=${ENUMERATE_PACKAGE}.HouseEnumerator
        ;;
    threetriangle)
        ENUMERATE_CLASS=${ENUMERATE_PACKAGE}.ThreeTriangleEnumerator
        ;;
    solarsquare)
        ENUMERATE_CLASS=${ENUMERATE_PACKAGE}.SolarSquareEnumerator
        ;;
    quadtriangle)
        ENUMERATE_CLASS=${ENUMERATE_PACKAGE}.QuadTriangleEnumerator
        ;;
    near5clique)
        ENUMERATE_CLASS=${ENUMERATE_PACKAGE}.Near5CliqueEnumerator
        ;;
    trianglecore)
        ENUMERATE_CLASS=${ENUMERATE_PACKAGE}.TriangleCoreEnumerator
        ;;
    starofdavidplus)
        ENUMERATE_CLASS=${ENUMERATE_PACKAGE}.StarOfDavidPlusEnumerator
        ;;
    twinclique4)
        ENUMERATE_CLASS=${ENUMERATE_PACKAGE}.TwinClique4Enumerator
        ;;
    twincsquare)
        ENUMERATE_CLASS=${ENUMERATE_PACKAGE}.TwinCSquareEnumerator
        ;;
    nothing)
        ENUMERATE_CLASS=cn.edu.nju.pasa.graph.analysis.subgraph.enumerator.DoNothingEnumerator
        ;;
    *)
        echo "unrecognized pattern:" $PATTERN
        exit 1
        ;;
esac

echo "GRAPH_FILE: " $GRAPH_FILE

hdfs dfs -rmr $OUTPUT_FILE

echo "----------START: " `date` "----------"
/usr/bin/timeout --kill-after=10s $timeout \
    hadoop jar \
		./SubgraphEnumeration-1-allinone.jar \
		cn.edu.nju.pasa.graph.analysis.subgraph.Driver \
        -D mapreduce.job.reduce.slowstart.completedmaps=0.90 \
        -D mapreduce.map.output.compress=true \
        -D mapreduce.map.output.compress.codec=org.apache.hadoop.io.compress.GzipCodec \
		-D mapreduce.map.memory.mb=4096 \
		-D mapreduce.reduce.memory.mb=$(($MEMORY_LIMIT_IN_GB * 1024)) \
		-D mapreduce.reduce.cpu.vcores=$((23)) \
		-D mapreduce.task.timeout=7200000 \
        -D mapreduce.reduce.java.opts="-XX:+PrintGCDetails -XX:+PrintGCTimeStamps -server -Xmx38g" \
        -files pasa.conf.prop \
		data.graph.path=$GRAPH_FILE \
		enumerator.class=$ENUMERATE_CLASS \
		blocking.queue.size=100 \
		num.working.threads=$THREAD_NUM \
		num.executors=$EXECUTORS_NUM \
		output.path=$OUTPUT_FILE \
        enable.enumerate=false \
		enable.load.balance=true \
		load.balance.threshold=500 \
        store.graph.to.db=false

retcode=$?
echo "----------END: " `date` "----------"
echo "Exit value: " $retcode

