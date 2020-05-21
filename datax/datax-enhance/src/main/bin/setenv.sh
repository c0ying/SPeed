#!/bin/sh
Cur_dir=$(dirname $0)

JRE_HOME='D:/jdk/jdk1.8.0_102/jre'

JAVA_GC_LOG="-XX:+HeapDumpOnOutOfMemoryError  -verbose:gc -XX:+PrintGCDetails -XX:+PrintGCDateStamps -XX:+PrintHeapAtGC  -XX:+UseGCLogFileRotation -XX:NumberOfGCLogFiles=5 -XX:GCLogFileSize=1M  -Xloggc:$Cur_dir/../logs/gc.log.`date +%Y%m%d%H%M%S`"

JAVA_OPTS="-Djava.awt.headless=true -Djava.net.preferIPv4Stack=true -server -Xms1g -Xmx1g -XX:ParallelGCThreads=4 -XX:MaxTenuringThreshold=4 -XX:+UseConcMarkSweepGC -XX:CMSInitiatingOccupancyFraction=73 -XX:+CMSParallelRemarkEnabled -XX:+UseFastAccessorMethods -XX:+AggressiveOpts -XX:+UseBiasedLocking -XX:+DisableExplicitGC"
