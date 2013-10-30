#!/bin/sh

TPCB_HOME="`dirname $0`/../"
TPCB_HOME=`cd $TPCB_HOME; pwd`

. $TPCB_HOME/config/config.sh

# Drop tables
sqlplus /nolog <<EOF
  SPOOL ${TEARDOWN_LOG} APPEND
  SET ECHO ON
  CONNECT ${TPCB_USER}/${TPCB_PASSWORD}@${TPCB_TNSNAME}
  @${TPCB_HOME}/sql/drop_table.sql
EOF
