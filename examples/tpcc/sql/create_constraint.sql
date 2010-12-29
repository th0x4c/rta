-- Create constrains
-- Primary key
-- Warehouse
ALTER TABLE warehouse ADD CONSTRAINT warehouse_pk
  PRIMARY KEY ( w_id )
  USING INDEX warehouse_pk;

-- District
ALTER TABLE district ADD CONSTRAINT district_pk
  PRIMARY KEY ( d_w_id, d_id )
  USING INDEX district_pk;

-- Customer
ALTER TABLE customer ADD CONSTRAINT customer_pk
  PRIMARY KEY ( c_w_id, c_d_id, c_id )
  USING INDEX customer_pk;

-- New-Order
ALTER TABLE new_order ADD CONSTRAINT new_order_pk
  PRIMARY KEY ( no_w_id, no_d_id, no_o_id )
  USING INDEX new_order_pk;

-- Order
ALTER TABLE orders ADD CONSTRAINT orders_pk
  PRIMARY KEY ( o_w_id, o_d_id, o_id )
  USING INDEX orders_pk;

-- Order-Line
ALTER TABLE order_line ADD CONSTRAINT order_line_pk
  PRIMARY KEY ( ol_w_id, ol_d_id, ol_o_id, ol_number )
  USING INDEX order_line_pk;

-- Item
ALTER TABLE item ADD CONSTRAINT item_pk
  PRIMARY KEY ( i_id )
  USING INDEX item_pk;

-- Stock
ALTER TABLE stock ADD CONSTRAINT stock_pk
  PRIMARY KEY ( s_w_id, s_i_id )
  USING INDEX stock_pk;

-- Foreign key
-- District
ALTER TABLE district ADD CONSTRAINT district_fk
  FOREIGN KEY ( d_w_id )
  REFERENCES warehouse ( w_id );

-- Customer
ALTER TABLE customer ADD CONSTRAINT customer_fk
  FOREIGN KEY ( c_w_id, c_d_id )
  REFERENCES district ( d_w_id, d_id );

-- History
ALTER TABLE history ADD CONSTRAINT history_fk1
  FOREIGN KEY ( h_c_w_id, h_c_d_id, h_c_id )
  REFERENCES customer ( c_w_id, c_d_id, c_id );

ALTER TABLE history ADD CONSTRAINT history_fk2
  FOREIGN KEY ( h_w_id, h_d_id )
  REFERENCES district ( d_w_id, d_id );

-- New-Order
ALTER TABLE new_order ADD CONSTRAINT new_order_fk
  FOREIGN KEY ( no_w_id, no_d_id, no_o_id )
  REFERENCES orders ( o_w_id, o_d_id, o_id );

-- Order
ALTER TABLE orders ADD CONSTRAINT orders_fk
  FOREIGN KEY ( o_w_id, o_d_id, o_c_id )
  REFERENCES customer ( c_w_id, c_d_id, c_id );

-- Order-Line
ALTER TABLE order_line ADD CONSTRAINT order_line_fk1
  FOREIGN KEY ( ol_w_id, ol_d_id, ol_o_id )
  REFERENCES orders ( o_w_id, o_d_id, o_id );

ALTER TABLE order_line ADD CONSTRAINT order_line_fk2
  FOREIGN KEY ( ol_supply_w_id, ol_i_id )
  REFERENCES stock ( s_w_id, s_i_id );

-- Stock
ALTER TABLE stock ADD CONSTRAINT stock_fk1
  FOREIGN KEY ( s_w_id )
  REFERENCES warehouse ( w_id );

ALTER TABLE stock ADD CONSTRAINT stock_fk2
  FOREIGN KEY ( s_i_id )
  REFERENCES item ( i_id );
