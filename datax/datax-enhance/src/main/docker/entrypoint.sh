#!/bin/sh

if [ ! -x $DATAX_HOME/log ]
then
  mkdir $DATAX_HOME/log
fi

if [ ! -x $DATAX_HOME/tmp ]
then
  mkdir $DATAX_HOME/tmp
fi

JAVA_OPTS="-Djava.awt.headless=true -Djava.net.preferIPv4Stack=true -server -Xms1g -Xmx1g -XX:ParallelGCThreads=4 -XX:MaxTenuringThreshold=4 -XX:+UseConcMarkSweepGC -XX:CMSInitiatingOccupancyFraction=73 -XX:+UseCMSCompactAtFullCollection -XX:+CMSParallelRemarkEnabled -XX:CMSFullGCsBeforeCompaction=4 -XX:+UseFastAccessorMethods -XX:+AggressiveOpts -XX:+UseBiasedLocking -XX:+DisableExplicitGC"

VM_OTHER_ARGS="-Dfile.encoding=UTF-8 -Ddatax.home=$DATAX_HOME"
OTHER_ARGS="start"
MAIN_CLASS="com.jingxin.framework.datax.enhance.core.HttpEntry"

$JAVA_HOME/bin/java $JAVA_OPTS -cp $DATAX_HOME/lib/*:$DATAX_HOME/lib:$DATAX_HOME/conf $VM_OTHER_ARGS $MAIN_CLASS $OTHER_ARGS
