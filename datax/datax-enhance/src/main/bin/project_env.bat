@echo off

set Cur_dir=%cd%

set VM_OTHER_ARGS=-Dfile.encoding=UTF-8 -Ddatax.home=%Cur_dir%\..\
set OTHER_ARGS=start
set PROJECT_NAME=SPeed
set MAIN_CLASS=com.jingxin.framework.datax.enhance.core.HttpEntry
