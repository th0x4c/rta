-- Create indexes

CREATE UNIQUE INDEX account_pk ON account ( account_id )
-- PCTFREE 1
TABLESPACE &account_index_tablespace;

CREATE UNIQUE INDEX teller_pk ON teller ( teller_id )
-- PCTFREE 1
TABLESPACE &teller_index_tablespace;

CREATE UNIQUE INDEX branch_pk ON branch ( branch_id )
-- PCTFREE 1
TABLESPACE &branch_index_tablespace;
