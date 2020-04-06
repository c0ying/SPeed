@echo off

set Cur_dir=%cd%

set JRE_HOME=D:\Java\jdk1.8.0_161\jre

set JAVA_OPTS=-Djava.awt.headless=true -Djava.net.preferIPv4Stack=true -server -Xms1g -Xmx1g -XX:ParallelGCThreads=2 -XX:MaxTenuringThreshold=2 -XX:+UseConcMarkSweepGC -XX:CMSInitiatingOccupancyFraction=73 -XX:+UseCMSCompactAtFullCollection -XX:+CMSParallelRemarkEnabled -XX:CMSFullGCsBeforeCompaction=4 -XX:+UseFastAccessorMethods -XX:+AggressiveOpts -XX:+UseBiasedLocking -XX:+DisableExplicitGC
