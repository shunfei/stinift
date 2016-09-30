#!/usr/bin/env bash
SCRIPT_DIR=$(cd $(dirname $0); echo $PWD)
ROOT_DIR=$(cd ${SCRIPT_DIR}/../;echo $PWD)

source ${ROOT_DIR}/conf/env.sh

java -DHADOOP_USER_NAME=${DHADOOP_USER_NAME} -cp ${ROOT_DIR}/conf:${ROOT_DIR}/lib/* com.sf.stinift.utils.StiniftMain $@
