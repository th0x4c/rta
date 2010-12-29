#!/bin/sh

TPCC_HOME="`dirname $0`/../"
echo $TPCC_HOME | grep "^/" > /dev/null 2>&1
if [ $? -ne 0 ]; then
  TPCC_HOME="`pwd`/$TPCC_HOME"
fi

. $TPCC_HOME/config/config.sh

# Drop tables
sqlplus /nolog <<EOF
  SPOOL ${TEARDOWN_LOG} APPEND
  SET ECHO ON
  CONNECT ${TPCC_USER}/${TPCC_PASSWORD}@${TPCC_TNSNAME}
  @${TPCC_HOME}/sql/drop_table.sql
EOF
