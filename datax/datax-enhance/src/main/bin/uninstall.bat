@echo off
 
cd..
set basedir=%CD%
set SERVICE_NAME=speed
set SRV=%Cur_dir%\speed-srv-x64.exe
echo ����ж�ط���...
"%SRV%" //DS//%SERVICE_NAME%
echo ����ж����ϡ�
pause