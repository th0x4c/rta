-- Create tables

CREATE TABLE account
(
  account_id       NUMBER(10,0),
  branch_id        NUMBER(10,0),
  account_balance  NUMBER(10,0),
  filler           VARCHAR2(97)
)
TABLESPACE &account_tablespace;

CREATE TABLE teller
(
  teller_id       NUMBER(10,0),
  branch_id       NUMBER(10,0),
  teller_balance  NUMBER(10,0),
  filler          CHAR(97)
)
-- PCTFREE 95
-- PCTUSED 4
TABLESPACE &teller_tablespace;

CREATE TABLE branch
(
  branch_id       NUMBER(10,0),
  branch_balance  NUMBER(10,0),
  filler          CHAR(98)
)
-- PCTFREE 95
-- PCTUSED 4
TABLESPACE &branch_tablespace;

CREATE TABLE history
(
  teller_id         NUMBER,
  branch_id         NUMBER,
  account_id        NUMBER,
  amount            NUMBER,
  timestamp         TIMESTAMP,
  filler            CHAR(39)
)
-- PCTFREE 1
-- PCTUSED 99
-- INITRANS 1
TABLESPACE &history_tablespace;
