#!/bin/sh

TPCC_HOME="`dirname $0`/../"
TPCC_HOME=`cd $TPCC_HOME; pwd`

. $TPCC_HOME/config/config.sh

# Create tables
sqlplus /nolog <<EOF
  SPOOL ${SETUP_LOG} APPEND
  SET ECHO ON
  CONNECT ${TPCC_USER}/${TPCC_PASSWORD}@${TPCC_TNSNAME}
  DEFINE table_tablespace = '${TABLE_TABLESPACE}'
  @${TPCC_HOME}/sql/create_table.sql
EOF

# Load data
${RTACTL} -p ${RTA_PORT} -n ${PARALLEL_LOAD_DEGREE} start ${TPCC_HOME}/script/load.rb 2>&1 | tee -a ${SETUP_LOG}

# Create indexes
sqlplus /nolog <<EOF
  SPOOL ${SETUP_LOG} APPEND
  SET ECHO ON
  CONNECT ${TPCC_USER}/${TPCC_PASSWORD}@${TPCC_TNSNAME}
  DEFINE index_tablespace = '${INDEX_TABLESPACE}'
  @${TPCC_HOME}/sql/create_index.sql
EOF

# Create constraints
sqlplus /nolog <<EOF
  SPOOL ${SETUP_LOG} APPEND
  SET ECHO ON
  CONNECT ${TPCC_USER}/${TPCC_PASSWORD}@${TPCC_TNSNAME}
  @${TPCC_HOME}/sql/create_constraint.sql
EOF

# Analyze
sqlplus /nolog <<EOF
  SPOOL ${SETUP_LOG} APPEND
  SET ECHO ON
  CONNECT ${TPCC_USER}/${TPCC_PASSWORD}@${TPCC_TNSNAME}
  DEFINE ownname = '${TPCC_USER}'
  @${TPCC_HOME}/sql/analyze.sql
EOF
