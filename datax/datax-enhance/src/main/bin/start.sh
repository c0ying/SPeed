#/bin/sh

Cur_dir=$(dirname $0)
. $Cur_dir/setenv.sh
. $Cur_dir/project_env.sh

if [ ! -x $Cur_dir/../log ]
then
  mkdir $Cur_dir/../log
fi

if [ ! -x $Cur_dir/../tmp ]
then
  mkdir $Cur_dir/../tmp
fi

nohup $JRE_HOME/bin/java $JAVA_OPTS -cp $Cur_dir/../lib/*:$Cur_dir/../lib:$Cur_dir/../conf $VM_OTHER_ARGS $MAIN_CLASS $OTHER_ARGS  >> $Cur_dir/../log/std.out 2>&1 &

pid_file=$Cur_dir/../tmp/run.pid
echo $! > $pid_file