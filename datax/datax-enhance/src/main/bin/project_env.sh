#!/bin/sh

Cur_dir=$(dirname $0)

VM_OTHER_ARGS="-Dfile.encoding=UTF-8 -Ddatax.home=$Cur_dir/../"
OTHER_ARGS="start"
PROJECT_NAME="speed"
MAIN_CLASS="com.jingxin.framework.datax.enhance.core.HttpEntry"
