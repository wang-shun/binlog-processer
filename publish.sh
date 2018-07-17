#!/bin/sh

mvn clean package -Dmaven.test.skip=true;


BROKERS="application1 application2 application3 application4 application5"
BINLOG_PROCESS_HOME="/data1/application/binlog-process"

cd binlog-main/target/binlog-main-1.0-SNAPSHOT/lib;

for broker in $BROKERS
do
    for file in $(ls -rt binlog*.jar);
    do
        scp -p $file root@$broker:$BINLOG_PROCESS_HOME/lib;
    done
done

#ssh root@application1 << EOF
 #   cd $BINLOG_PROCESS_HOME/bin
   # sh cluster_stop.sh
   # sh cluster_start.sh
 #   exit;
# EOF








