-- Create indexes

-- Warehouse
CREATE UNIQUE INDEX warehouse_pk
  ON warehouse ( w_id )
  TABLESPACE &index_tablespace;

-- District
CREATE UNIQUE INDEX district_pk
  ON district ( d_w_id, d_id )
  TABLESPACE &index_tablespace;

-- Customer
CREATE UNIQUE INDEX customer_pk
  ON customer ( c_w_id, c_d_id, c_id )
  TABLESPACE &index_tablespace;

-- New-Order
CREATE UNIQUE INDEX new_order_pk
  ON new_order ( no_w_id, no_d_id, no_o_id )
  TABLESPACE &index_tablespace;

-- Order
CREATE UNIQUE INDEX orders_pk
  ON orders ( o_w_id, o_d_id, o_id )
  TABLESPACE &index_tablespace;

-- Order-Line
CREATE UNIQUE INDEX order_line_pk
  ON order_line ( ol_w_id, ol_d_id, ol_o_id, ol_number )
  TABLESPACE &index_tablespace;

-- Item
CREATE UNIQUE INDEX item_pk
  ON item ( i_id )
  TABLESPACE &index_tablespace;

-- Stock
CREATE UNIQUE INDEX stock_pk
  ON stock ( s_w_id, s_i_id )
  TABLESPACE &index_tablespace;

-- Additional index

-- Customer
CREATE UNIQUE INDEX customer_idx
  ON customer ( c_last, c_w_id, c_d_id, c_first, c_id )
  TABLESPACE &index_tablespace;

-- Order
CREATE UNIQUE INDEX orders_idx
  ON orders ( o_c_id, o_d_id, o_w_id, o_id )
  TABLESPACE &index_tablespace;
