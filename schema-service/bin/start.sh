#!/bin/bash
export APP_HOME="$(dirname $0)/../"
LIB_HOME=${APP_HOME}/lib
CONF_DIR=${APP_HOME}/conf
JAVA_OPTS="-Xmx1G "
echo "start process with command: java -Dapp.conf.dir=${CONF_DIR} -classpath ${LIB_HOME}/*:${CONF_DIR} Main"
java -Dapp.conf.dir=${CONF_DIR} -classpath ${LIB_HOME}/*:${CONF_DIR} com.tree.net.bigdata.schema.Main
