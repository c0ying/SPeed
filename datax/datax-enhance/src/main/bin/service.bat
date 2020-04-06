@echo off
 
rem 设置程序名称
set SERVICE_EN_NAME=speed
set SERVICE_CH_NAME=speed

rem 设置程序依赖及程序入口类
set Cur_dir=%cd%
call %Cur_dir%\setenv.bat

rem 设置java路径
set JAVA_HOME=%JRE_HOME%
 

set BASEDIR=%Cur_dir%\..
set CLASSPATH=%BASEDIR%\lib\*;%BASEDIR%\lib;%BASEDIR%\conf
set MAIN_CLASS=com.jingxin.framework.speed.Bootstrap
set JAVA_OPTS=-Djava.awt.headless=true -Djava.net.preferIPv4Stack=true -XX:ParallelGCThreads=2 -XX:MaxTenuringThreshold=2 -XX:+UseConcMarkSweepGC -XX:CMSInitiatingOccupancyFraction=73 -XX:+UseCMSCompactAtFullCollection -XX:+CMSParallelRemarkEnabled -XX:CMSFullGCsBeforeCompaction=2 -XX:+UseFastAccessorMethods -XX:+AggressiveOpts -XX:+UseBiasedLocking -XX:+DisableExplicitGC
rem 设置prunsrv路径
set SRV=%Cur_dir%\speed-srv-x64.exe

rem 设置日志路径及日志文件前缀
set LOGPATH=%BASEDIR%\log
 
rem 输出信息
echo SERVICE_NAME: %SERVICE_EN_NAME%
echo JAVA_HOME: %JAVA_HOME%
echo MAIN_CLASS: %MAIN_CLASS%
echo prunsrv path: %SRV%
 
rem 设置jvm
if "%JVM%" == "" goto findJvm
if exist "%JVM%" goto foundJvm
:findJvm
set "JVM=%JAVA_HOME%\jre\bin\server\jvm.dll"
if exist "%JVM%" goto foundJvm
echo can not find jvm.dll automatically,
echo please use COMMAND to localation it
echo then install service
goto end
:foundJvm
echo 正在安装服务...
rem 安装
"%SRV%" //IS//%SERVICE_EN_NAME% --DisplayName="%SERVICE_CH_NAME%" "--Classpath=%CLASSPATH%" "--Install=%SRV%" "--JavaHome=%JAVA_HOME%" "--Jvm=%JVM%" --JvmMs=256 --JvmMx=1024 --Startup=auto --StartMode=jvm --StartClass=%MAIN_CLASS% --StartMethod=start --StopMode=jvm --StopClass=%MAIN_CLASS% --StopParams=stop --LogPath=%LOGPATH% --StdOutput=auto --StdError=auto
echo 安装服务完成。
pause