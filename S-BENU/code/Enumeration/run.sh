#!/bin/bash
GRAPH_FILE=wangshen/$1/resort
ENUMERATE_CLASS=cn.edu.nju.pasa.graph.analysis.subgraph.enumerator.DoNothingEnumerator
ENUMERATE_CLASS=cn.edu.nju.pasa.graph.analysis.subgraph.enumerator.SquareEnumerator
OUTPUT_FILE=/wzk/subenu-output
MEMORY_LIMIT_IN_GB=30
THREAD_NUM=24

hdfs dfs -rmr $OUTPUT_FILE

hadoop jar \
		./SubgraphEnumeration-1-allinone.jar \
		cn.edu.nju.pasa.graph.analysis.subgraph.Driver \
		-D mapreduce.reduce.memory.mb=$(($MEMORY_LIMIT_IN_GB * 1024)) \
		-D mapreduce.reduce.cpu.vcores=$(($THREAD_NUM-1)) \
		-files pasa.conf.prop \
		data.graph.path=$GRAPH_FILE \
		enumerator.class=$ENUMERATE_CLASS \
		blocking.queue.size=100 \
		enumerator.class=$ENUMERATE_CLASS \
		num.working.threads=$THREAD_NUM \
		num.executors=16 \
		output.path=$OUTPUT_FILE \




