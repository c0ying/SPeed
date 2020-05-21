@echo off

set Cur_dir=%cd%
call %Cur_dir%\setenv.bat
call %Cur_dir%\project_env.bat

call %JRE_HOME%\bin\java %JAVA_OPTS% -cp %Cur_dir%\..\lib\*;%Cur_dir%\..\lib;%Cur_dir%\..\conf %VM_OTHER_ARGS% %MAIN_CLASS% %OTHER_ARGS%
