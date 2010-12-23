-- Create constrains
-- Primary key

ALTER TABLE account ADD CONSTRAINT account_pk PRIMARY KEY ( account_id )
USING INDEX account_pk;

ALTER TABLE teller ADD CONSTRAINT teller_pk PRIMARY KEY ( teller_id )
USING INDEX teller_pk;

ALTER TABLE branch ADD CONSTRAINT branch_pk PRIMARY KEY ( branch_id )
USING INDEX branch_pk;

-- Foreign key

ALTER TABLE account ADD CONSTRAINT account_fk FOREIGN KEY ( branch_id )
REFERENCES branch ( branch_id );

ALTER TABLE teller ADD CONSTRAINT teller_fk FOREIGN KEY ( branch_id )
REFERENCES branch ( branch_id );

ALTER TABLE history ADD CONSTRAINT history_fk1 FOREIGN KEY ( branch_id )
REFERENCES branch ( branch_id );

ALTER TABLE history ADD CONSTRAINT history_fk2 FOREIGN KEY ( teller_id )
REFERENCES teller ( teller_id );

ALTER TABLE history ADD CONSTRAINT history_fk3 FOREIGN KEY ( account_id )
REFERENCES account ( account_id );
