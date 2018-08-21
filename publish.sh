#!/bin/sh

#mvn clean package -Dmaven.test.skip=true;


BROKERS="application1 application2 application3 application4 application5"
BINLOG_PROCESS_HOME="/data1/application/binlog-process"

cd binlog-file/target/binlog-file-1.0-SNAPSHOT/bin;

for broker in $BROKERS
do
    for file in $(ls -rt resolve*.sh);
    do
        scp -p $file root@$broker:$BINLOG_PROCESS_HOME/bin;
    done
done

#ssh root@application1 << EOF
 #   cd $BINLOG_PROCESS_HOME/bin
   # sh cluster_stop.sh
   # sh cluster_start.sh
 #   exit;
# EOF








