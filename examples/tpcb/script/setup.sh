#!/bin/sh

TPCB_HOME="`dirname $0`/../"
echo $TPCB_HOME | grep "^/" > /dev/null 2>&1
if [ $? -ne 0 ]; then
  TPCB_HOME="`pwd`/$TPCB_HOME"
fi

. $TPCB_HOME/config/config.sh

# Create tables
sqlplus /nolog <<EOF
  SPOOL ${SETUP_LOG} APPEND
  SET ECHO ON
  CONNECT ${TPCB_USER}/${TPCB_PASSWORD}@${TPCB_TNSNAME}
  DEFINE account_tablespace = '${ACCOUNT_TABLESPACE}'
  DEFINE teller_tablespace = '${TELLER_TABLESPACE}'
  DEFINE branch_tablespace = '${BRANCH_TABLESPACE}'
  DEFINE history_tablespace = '${HISTORY_TABLESPACE}'
  @${TPCB_HOME}/sql/create_table.sql
EOF

# Load data
${RTACTL} -p ${RTA_PORT} -n ${PARALLEL_LOAD_DEGREE} start ${TPCB_HOME}/script/load.rb 2>&1 | tee -a ${SETUP_LOG}

# Create indexes
sqlplus /nolog <<EOF
  SPOOL ${SETUP_LOG} APPEND
  SET ECHO ON
  CONNECT ${TPCB_USER}/${TPCB_PASSWORD}@${TPCB_TNSNAME}
  DEFINE account_index_tablespace = '${ACCOUNT_INDEX_TABLESPACE}'
  DEFINE teller_index_tablespace = '${TELLER_INDEX_TABLESPACE}'
  DEFINE branch_index_tablespace = '${BRANCH_INDEX_TABLESPACE}'
  @${TPCB_HOME}/sql/create_index.sql
EOF

# Create constraints
sqlplus /nolog <<EOF
  SPOOL ${SETUP_LOG} APPEND
  SET ECHO ON
  CONNECT ${TPCB_USER}/${TPCB_PASSWORD}@${TPCB_TNSNAME}
  @${TPCB_HOME}/sql/create_constraint.sql
EOF

# Analyze
sqlplus /nolog <<EOF
  SPOOL ${SETUP_LOG} APPEND
  SET ECHO ON
  CONNECT ${TPCB_USER}/${TPCB_PASSWORD}@${TPCB_TNSNAME}
  DEFINE ownname = '${TPCB_USER}'
  @${TPCB_HOME}/sql/analyze.sql
EOF
