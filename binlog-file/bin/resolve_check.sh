#!/bin/bash

#set -x
cd `dirname $0`
BIN_DIR=`pwd`
cd ..
DEPLOY_DIR=`pwd`
CONF_DIR=${DEPLOY_DIR}/conf
LOG_DIR=${DEPLOY_DIR}/logs
LIB_DIR=${DEPLOY_DIR}/lib
LIB_JARS=`ls ${LIB_DIR}|grep .jar|awk '{print "'${LIB_DIR}'/"$0}'|tr "\n" ":"`
LIB_JARS=${LIB_JARS}
LOG_FILE=${LOG_DIR}/resolve_check.log
APP_MAIN_CLASS=com.datatrees.datacenter.check.ResolveCheck

start()
{

        if [ ! -d ${LOG_DIR} ]; then
                mkdir ${LOG_DIR}
        fi

        if [ ! -e ${CONF_DIR}/binlog.properties ]; then
                echo "${CONF_DIR}/binlog.properties does not exist "
                exit 0
        fi

        checkpid

        if [ ${psid} -ne 0 ]; then
                echo "==========================="
                echo "warn: $APP_MAIN_CLASS already started! (pid=$psid) "
                echo "==========================="
        else
                echo "binlog process is starting ..."

                nohup java -server -Xms2g -Xmx4g -classpath ${CONF_DIR}:${LIB_JARS} ${APP_MAIN_CLASS} $2 > ${LOG_FILE} 2>&1 &

                checkpid

                if [ ${psid} -ne 0 ]; then
                        echo "(pid=$psid) [OK]"
                else
                        echo "[Failed]"
                fi
        fi

}

checkpid()
{
        javaps=`ps -ef | grep ${APP_MAIN_CLASS} | grep -v "grep"`
        if [ -n "$javaps" ]; then
                psid=`echo ${javaps} | awk '{print $2}'`
        else
                psid=0
        fi
}

stop()
{

        checkpid

        if [ ${psid} -ne 0 ]; then
                echo -n "Stopping $APP_MAIN_CLASS ...(pid=$psid) "
                kill -9 ${psid}
                if [ $? -eq 0 ]; then
                        echo "[OK]"
                else
                        echo "[Failed]"
                fi

                checkpid
                if [ ${psid} -ne 0 ]; then
                        stop
                fi
        else
                echo "================================"
                echo "warn: $APP_MAIN_CLASS is not running"
                echo "================================"
        fi

}

status()
{
        checkpid

        if [ ${psid} -ne 0 ];  then
                echo "$APP_MAIN_CLASS is running! (pid=$psid)"
        else
                echo "$APP_MAIN_CLASS is not running"
        fi
}

case "$1" in
   'start')
      start $2
     ;;
   'stop')
     stop
     ;;
   'restart')
     stop
     start
     ;;
   'status')
     status
     ;;
  *)
     echo "Usage: $0 {start|stop|restart|status|info} {filePath[eg:/data/warehouse/update]}"
     exit 1
esac