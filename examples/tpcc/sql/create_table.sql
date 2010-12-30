-- Create tables

-- Warehouse
CREATE TABLE warehouse
(
  w_id       NUMBER,       -- 2*W unique IDs
  w_name     VARCHAR2(10),
  w_street_1 VARCHAR2(20),
  w_street_2 VARCHAR2(20),
  w_city     VARCHAR2(20),
  w_state    CHAR(2),
  w_zip      CHAR(9),
  w_tax      NUMBER(4, 4),
  w_ytd      NUMBER(12, 2)
)
TABLESPACE &table_tablespace;

-- District
CREATE TABLE district
(
  d_id        NUMBER(2, 0), -- 20 unique IDs
  d_w_id      NUMBER,       -- 2*W unique IDs
  d_name      VARCHAR2(10),
  d_street_1  VARCHAR2(20),
  d_street_2  VARCHAR2(20),
  d_city      VARCHAR2(20),
  d_state     CHAR(2),
  d_zip       CHAR(9),
  d_tax       NUMBER(4, 4),
  d_ytd       NUMBER(12, 2),
  d_next_o_id NUMBER(8, 0)  -- 10,000,000 unique IDs
)
TABLESPACE &table_tablespace;

-- Customer
CREATE TABLE customer
(
  c_id           NUMBER(5, 0), -- 96,000 unique IDs
  c_d_id         NUMBER(2, 0), -- 20 unique IDs
  c_w_id         NUMBER,       -- 2*W unique IDs
  c_first        VARCHAR2(16),
  c_middle       CHAR(2),
  c_last         VARCHAR2(16),
  c_street_1     VARCHAR2(20),
  c_street_2     VARCHAR2(20),
  c_city         VARCHAR2(20),
  c_state        CHAR(2),
  c_zip          CHAR(9),
  c_phone        CHAR(16),
  c_since        DATE,
  c_credit       CHAR(2),
  c_credit_lim   NUMBER(12, 2),
  c_discount     NUMBER(4, 4),
  c_balance      NUMBER(12, 2),
  c_ytd_payment  NUMBER(12, 2),
  c_payment_cnt  NUMBER(4, 0),
  c_delivery_cnt NUMBER(4, 0),
  c_data         VARCHAR2(500)
)
TABLESPACE &table_tablespace;

-- History
CREATE TABLE history
(
  h_c_id   NUMBER(5, 0), -- 96,000 unique IDs
  h_c_d_id NUMBER(2, 0), -- 20 unique IDs
  h_c_w_id NUMBER,       -- 2*W unique IDs
  h_d_id   NUMBER(2, 0), -- 20 unique IDs
  h_w_id   NUMBER,       -- 2*W unique IDs
  h_date   DATE,
  h_amount NUMBER(6, 2),
  h_data   VARCHAR2(24)
)
TABLESPACE &table_tablespace;

-- New-Order
CREATE TABLE new_order
(
  no_o_id NUMBER(8, 0), -- 10,000,000 unique IDs
  no_d_id NUMBER(2, 0), -- 20 unique IDs
  no_w_id NUMBER        -- 2*W unique IDs
)
TABLESPACE &table_tablespace;

-- Order
CREATE TABLE orders
(
  o_id         NUMBER(8, 0), -- 10,000,000 unique IDs
  o_d_id       NUMBER(2, 0), -- 20 unique IDs
  o_w_id       NUMBER,       -- 2*W unique IDs
  o_c_id       NUMBER(5, 0), -- 96,000 unique IDs
  o_entry_d    DATE,
  o_carrier_id NUMBER(2, 0), -- 10 unique IDs, or null
  o_ol_cnt     NUMBER(2, 0),
  o_all_local  NUMBER(1, 0)
)
TABLESPACE &table_tablespace;

-- Order-Line
CREATE TABLE order_line
(
  ol_o_id        NUMBER(8, 0), -- 10,000,000 unique IDs
  ol_d_id        NUMBER(2, 0), -- 20 unique IDs
  ol_w_id        NUMBER,       -- 2*W unique IDs
  ol_number      NUMBER(2, 0), -- 15 unique IDs
  ol_i_id        NUMBER(6, 0), -- 200,000 unique IDs
  ol_supply_w_id NUMBER,       -- 2*W unique IDs
  ol_delivery_d  DATE,         -- date and time, or null
  ol_quantity    NUMBER(2, 0),
  ol_amount      NUMBER(6, 2),
  ol_dist_info   CHAR(24)
)
TABLESPACE &table_tablespace;

-- Item
CREATE TABLE item
(
  i_id    NUMBER(6, 0), -- 200,000 unique IDs
  i_im_id NUMBER(6, 0), -- 200,000 unique IDs
  i_name  VARCHAR2(24),
  i_price NUMBER(5, 2),
  i_data  VARCHAR2(50)
)
TABLESPACE &table_tablespace;

-- Stock
CREATE TABLE stock
(
  s_i_id       NUMBER(6, 0), -- 200,000 unique IDs
  s_w_id       NUMBER,       -- 2*W unique IDs
  s_quantity   NUMBER(4, 0),
  s_dist_01    CHAR(24),
  s_dist_02    CHAR(24),
  s_dist_03    CHAR(24),
  s_dist_04    CHAR(24),
  s_dist_05    CHAR(24),
  s_dist_06    CHAR(24),
  s_dist_07    CHAR(24),
  s_dist_08    CHAR(24),
  s_dist_09    CHAR(24),
  s_dist_10    CHAR(24),
  s_ytd        NUMBER(8, 0),
  s_order_cnt  NUMBER(4, 0),
  s_remote_cnt NUMBER(4, 0),
  s_data       VARCHAR2(50)
)
TABLESPACE &table_tablespace;
