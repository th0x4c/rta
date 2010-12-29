-- Analyze

EXEC  DBMS_STATS.GATHER_TABLE_STATS(ownname => '&&ownname', -
                                    tabname => 'WAREHOUSE', -
                                    cascade => true);

EXEC  DBMS_STATS.GATHER_TABLE_STATS(ownname => '&&ownname', -
                                    tabname => 'DISTRICT', -
                                    cascade => true);

EXEC  DBMS_STATS.GATHER_TABLE_STATS(ownname => '&&ownname', -
                                    tabname => 'CUSTOMER', -
                                    cascade => true);

EXEC  DBMS_STATS.GATHER_TABLE_STATS(ownname => '&&ownname', -
                                    tabname => 'HISTORY', -
                                    cascade => true);

EXEC  DBMS_STATS.GATHER_TABLE_STATS(ownname => '&&ownname', -
                                    tabname => 'NEW_ORDER', -
                                    cascade => true);

EXEC  DBMS_STATS.GATHER_TABLE_STATS(ownname => '&&ownname', -
                                    tabname => 'ORDERS', -
                                    cascade => true);

EXEC  DBMS_STATS.GATHER_TABLE_STATS(ownname => '&&ownname', -
                                    tabname => 'ORDER_LINE', -
                                    cascade => true);

EXEC  DBMS_STATS.GATHER_TABLE_STATS(ownname => '&&ownname', -
                                    tabname => 'ITEM', -
                                    cascade => true);

EXEC  DBMS_STATS.GATHER_TABLE_STATS(ownname => '&&ownname', -
                                    tabname => 'STOCK', -
                                    cascade => true);
