#!/bin/sh

TPCC_HOME="`dirname $0`/../"
TPCC_HOME=`cd $TPCC_HOME; pwd`

. $TPCC_HOME/config/config.sh

# Drop tables
sqlplus /nolog <<EOF
  SPOOL ${TEARDOWN_LOG} APPEND
  SET ECHO ON
  CONNECT ${TPCC_USER}/${TPCC_PASSWORD}@${TPCC_TNSNAME}
  @${TPCC_HOME}/sql/drop_table.sql
EOF
