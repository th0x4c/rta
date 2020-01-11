#!/bin/sh

TPCH_HOME="`dirname $0`/../"
TPCH_HOME=`cd $TPCH_HOME; pwd`

. $TPCH_HOME/config/config.sh

TPCH_LOG=$TPCH_HOME/log/tpch.log

OS_DBGEN_DIR=$OS_DIRECTORY/`basename $TPCH_DBGEN_ZIP .zip`/dbgen
SEED=`date +%m%d%H%M%S`

generate_data_for_refresh_function ()
{
  local refresh_count=0
  if [ -f $TPCH_HOME/config/refresh_count ]
  then
    refresh_count=`cat $TPCH_HOME/config/refresh_count`
  else
    echo 0 > $TPCH_HOME/config/refresh_count
  fi
  local begin_refresh_nth=`expr $refresh_count + 1`
  local end_refresh_nth=`expr $begin_refresh_nth + $STREAMS`
  local cmd=`cat <<EOF
               cd $OS_DBGEN_DIR; \
               ./dbgen -s $SCALE_FACTOR -U $end_refresh_nth -S $begin_refresh_nth; \
               mv ./*.tbl.u* ./delete.* $OS_DIRECTORY
EOF
`
  $SSHPASS ssh -l $OS_USER $OS_HOSTNAME $cmd
}

generate_query()
{
  local cmd=`cat <<EOF
               mkdir -p $OS_DIRECTORY/query; \
               cd $OS_DBGEN_DIR/queries; \
               seed=$SEED; \
               i=0; \
               while [ \\$i -le $STREAMS ]; \
               do \
                 ../qgen -b $OS_DBGEN_DIR/dists.dss -c -r \\$seed -p \\$i -s $SCALE_FACTOR -l query_parameters.\\$i > $OS_DIRECTORY/query/query.\\$i; \
                 seed=\\\`expr \\$seed + 1\\\`; \
                 i=\\\`expr \\$i + 1\\\`; \
               done
EOF
`

  $SSHPASS ssh -l $OS_USER $OS_HOSTNAME $cmd
}

get_query()
{
  $SSHPASS scp -r ${OS_USER}@${OS_HOSTNAME}:${OS_DIRECTORY}/query $TPCH_HOME
}

generate_data_for_refresh_function
generate_query
get_query

${RTACTL} -p ${RTA_PORT} -n ${STREAMS} start ${TPCH_HOME}/tpch.rb 2>&1 | tee -a ${TPCH_LOG}
