#!/bin/sh

TPCH_HOME="`dirname $0`/../"
TPCH_HOME=`cd $TPCH_HOME; pwd`

. $TPCH_HOME/config/config.sh

OS_DBGEN_DIR=$OS_DIRECTORY/`basename $TPCH_DBGEN_ZIP .zip`/dbgen

get_dbgen()
{
  if [ `type -p curl` != "" ]
  then
    HTTP_CLIENT="curl -f -L -o"
  else
    HTTP_CLIENT="wget -O"
  fi

  $HTTP_CLIENT $TPCH_HOME/$TPCH_DBGEN_ZIP $TPCH_DBGEN_URL
}

make_dbgen()
{
  local cmd=`cat <<EOF
               cd $OS_DIRECTORY; \
               unzip -u -o $TPCH_DBGEN_ZIP; \
               cd $OS_DBGEN_DIR; \
               make -f makefile.suite CC=gcc DATABASE=ORACLE MACHINE=LINUX WORKLOAD=TPCH
EOF
`

  $SSHPASS ssh -l $OS_USER $OS_HOSTNAME $cmd
}

setup_dbgen()
{
  if [ ! -f $TPCH_HOME/$TPCH_DBGEN_ZIP ]
  then
    get_dbgen
  fi

  $SSHPASS ssh -l $OS_USER $OS_HOSTNAME mkdir -p $OS_DIRECTORY

  $SSHPASS scp $TPCH_HOME/$TPCH_DBGEN_ZIP ${OS_USER}@${OS_HOSTNAME}:${OS_DIRECTORY}

  make_dbgen
}

generate_data()
{
  local cmd=`cat <<EOF
               cd $OS_DBGEN_DIR; \
               step=1; \
               while [ \\$step -le $PARALLEL_DEGREE ]; \
               do \
                 if [ $PARALLEL_DEGREE -gt 1 ]; \
                 then \
                   ./dbgen -f -s $SCALE_FACTOR -C $PARALLEL_DEGREE -S \\$step; \
                 else \
                   ./dbgen -f -s $SCALE_FACTOR; \
                 fi; \
                 step=\\\`expr \\$step + 1\\\`; \
               done; \
               mv ./*.tbl* $OS_DIRECTORY
EOF
`
  $SSHPASS ssh -l $OS_USER $OS_HOSTNAME $cmd
}

setup_dbgen
generate_data
