-- Analyze

EXEC  DBMS_STATS.GATHER_TABLE_STATS(ownname => '&&ownname', -
                                    tabname => 'ACCOUNT', -
                                    cascade => true);

EXEC  DBMS_STATS.GATHER_TABLE_STATS(ownname => '&&ownname', -
                                    tabname => 'TELLER', -
                                    cascade => true);

EXEC  DBMS_STATS.GATHER_TABLE_STATS(ownname => '&&ownname', -
                                    tabname => 'BRANCH', -
                                    cascade => true);
