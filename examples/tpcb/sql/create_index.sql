-- Create indexes

CREATE UNIQUE INDEX account_pk on account ( account_id )
-- PCTFREE 1
TABLESPACE &account_index_tablespace;

CREATE UNIQUE INDEX teller_pk on teller ( teller_id )
-- PCTFREE 1
TABLESPACE &teller_index_tablespace;

CREATE UNIQUE INDEX branch_pk on branch ( branch_id )
-- PCTFREE 1
TABLESPACE &branch_index_tablespace;
