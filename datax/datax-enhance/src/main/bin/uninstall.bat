@echo off
 
cd..
set basedir=%CD%
set SERVICE_NAME=speed
set SRV=%Cur_dir%\speed-srv-x64.exe
echo 正在卸载服务...
"%SRV%" //DS//%SERVICE_NAME%
echo 服务卸载完毕。
pause