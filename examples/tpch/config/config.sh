# Configuration for setup, tpch and teardown script

OS_HOSTNAME=sv1.local
OS_USER=oracle
OS_DIRECTORY=/home/oracle/rta_tpch

TPCH_DBGEN_ZIP=tpch_2_16_1.zip
TPCH_DBGEN_URL=http://www.tpc.org/tpch/spec/$TPCH_DBGEN_ZIP

RTACTL=$TPCH_HOME/../../bin/rtactl
RTA_PORT=9000

SCALE_FACTOR=1      # 1, 10, 30, 100, 300, 1000, 3000, 10000, 30000, 100000
PARALLEL_DEGREE=4

STREAMS=2
