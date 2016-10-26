#!/usr/bin/env bash
SCRIPT_DIR=$(cd $(dirname $0); echo $PWD)
ROOT_DIR=$(cd ${SCRIPT_DIR}/../;echo $PWD)

source ${ROOT_DIR}/conf/env.sh

JVMOPTS="-Xmx512m -XX:+PrintGCDetails -XX:+PrintGCTimeStamps"

java ${JVMOPTS} -DHADOOP_USER_NAME=${HADOOP_USER_NAME} -cp ${ROOT_DIR}/conf:${ROOT_DIR}/lib/* com.sf.stinift.utils.StiniftMain $@