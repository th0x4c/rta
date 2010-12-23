#!/bin/sh

TPCB_HOME="`dirname $0`/../"
echo $TPCB_HOME | grep "^/" > /dev/null 2>&1
if [ $? -ne 0 ]; then
  TPCB_HOME="`pwd`/$TPCB_HOME"
fi

. $TPCB_HOME/config/config.sh

# Drop tables
sqlplus /nolog <<EOF
  SPOOL ${TEARDOWN_LOG} APPEND
  SET ECHO ON
  CONNECT ${TPCB_USER}/${TPCB_PASSWORD}@${TPCB_TNSNAME}
  @${TPCB_HOME}/sql/drop_table.sql
EOF
