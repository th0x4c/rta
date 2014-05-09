#!/bin/sh

TPCH_HOME="`dirname $0`/../"
TPCH_HOME=`cd $TPCH_HOME; pwd`

. $TPCH_HOME/config/config.sh

TEARDOWN_LOG=$TPCH_HOME/log/teardown.log

rm -f $TPCH_HOME/config/refresh_count

${RTACTL} -p ${RTA_PORT} start ${TPCH_HOME}/script/teardown.rb 2>&1 | tee -a ${TEARDOWN_LOG}
