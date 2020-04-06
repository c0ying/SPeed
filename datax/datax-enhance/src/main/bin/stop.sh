#!/bin/sh

Cur_dir=$(dirname $0)
. $Cur_dir/setenv.sh

kill $(more $Cur_dir/../tmp/run.pid)
