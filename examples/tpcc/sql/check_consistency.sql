-- Condition 1: W_YTD = sum(D_YTD)
WITH x AS (SELECT w.w_id, w.w_ytd, d.sum_d_ytd
           FROM warehouse w,
                (SELECT d_w_id, SUM(d_ytd) sum_d_ytd
                 FROM district
                 GROUP BY d_w_id) d
           WHERE w.w_id = d.d_w_id)
SELECT * FROM x
MINUS
SELECT * FROM x
WHERE w_ytd = sum_d_ytd
/

-- Condition 2: D_NEXT_O_ID - 1 = max(O_ID) = max(NO_O_ID)
WITH x AS (SELECT d.d_w_id, d.d_id, d.d_next_o_id, o.max_o_id, no.max_no_o_id
           FROM district d,
                (SELECT o_w_id, o_d_id, MAX(o_id) max_o_id
                 FROM orders
                 GROUP BY o_w_id, o_d_id) o,
                (SELECT no_w_id, no_d_id, MAX(no_o_id) max_no_o_id
                 FROM new_order
                 GROUP BY no_w_id, no_d_id) no
           WHERE d.d_w_id = o.o_w_id AND d.d_w_id = no.no_w_id AND
                 d.d_id = o.o_d_id AND d.d_id = no.no_d_id)
SELECT * FROM x
MINUS
SELECT * FROM x
WHERE d_next_o_id - 1 = max_o_id AND d_next_o_id - 1 = max_no_o_id
/

-- Condition 3: max(NO_O_ID) - min(NO_O_ID) + 1 
--              = [number of rows in the NEW-ORDER table for this district]
WITH x AS (SELECT no_w_id, no_d_id, MAX(no_o_id) max_no_o_id,
                  MIN(no_o_id) min_no_o_id, COUNT(*) count_no
           FROM new_order
           GROUP BY no_w_id, no_d_Id)
SELECT * FROM x
MINUS
SELECT * FROM x
WHERE max_no_o_id - min_no_o_id + 1 = count_no
/

-- Condition 4: sum(O_OL_CNT)
--              = [number of rows in the ORDER-LINE table for this district]
WITH x AS (SELECT o.o_w_id, o.o_d_id, o.sum_o_ol_cnt, ol.count_ol
           FROM (SELECT o_w_id, o_d_id, SUM(o_ol_cnt) sum_o_ol_cnt
                 FROM orders
                 GROUP BY o_w_id, o_d_id) o,
                (SELECT ol_w_id, ol_d_id, COUNT(*) count_ol
                 FROM order_line
                 GROUP BY ol_w_id, ol_d_id) ol
           WHERE o.o_w_id = ol.ol_w_id AND
                 o.o_d_id = ol.ol_d_id)
SELECT * FROM x
MINUS
SELECT * FROM x
WHERE sum_o_ol_cnt = count_ol
/

-- Condition 5: For any row in the ORDER table, O_CARRIER_ID is set to a null
--              value if and only if there is a corresponding row in the 
--              NEW-ORDER table
WITH x AS (SELECT o.o_w_id, o.o_d_id, o.o_id, o.o_carrier_id, no.count_no
           FROM orders o,
                (SELECT no_w_id, no_d_id, no_o_id, COUNT(*) count_no
                 FROM new_order
                 GROUP BY no_w_id, no_d_id, no_o_id) no
           WHERE o.o_w_id = no.no_w_id AND
                 o.o_d_id = no.no_d_id AND
                 o.o_id = no.no_o_id)
SELECT * FROM x
MINUS
SELECT * FROM x
WHERE (o_carrier_id IS NULL AND count_no != 0) OR
      (o_carrier_id IS NOT NULL AND count_no = 0)
/

-- Condition 6: For any row in the ORDER table, O_OL_CNT must equal the number
--              of rows in the ORDER-LINE table for the corresponding order
WITH x AS (SELECT o.o_w_id, o.o_d_id, o.o_id, o.o_ol_cnt, ol.count_ol
           FROM orders o,
                (SELECT ol_w_id, ol_d_id, ol_o_id, COUNT(*) count_ol
                 FROM order_line
                 GROUP BY ol_w_id, ol_d_id, ol_o_id) ol
           WHERE o.o_w_id = ol.ol_w_id AND
                 o.o_d_id = ol.ol_d_id AND
                 o.o_id = ol.ol_o_id)
SELECT * FROM x
MINUS
SELECT * FROM x
WHERE o_ol_cnt = count_ol
/

-- Condition 7: For any row in the ORDER-LINE table, OL_DELIVERY_D is set to
--              a null date/time if and only if the corresponding row in the
--              ORDER table has O_CARRIER_ID set to a null value
WITH x AS (SELECT ol.ol_w_id, ol.ol_d_id, ol.ol_o_id, ol.ol_delivery_d,
                  o.o_carrier_id
           FROM order_line ol,
                orders o
           WHERE ol.ol_w_id = o.o_w_id AND
                 ol.ol_d_id = o.o_d_id AND
                 ol.ol_o_id = o.o_id)
SELECT * FROM x
MINUS
SELECT * FROM x
WHERE (ol_delivery_d IS NULL AND o_carrier_id IS NULL) OR
      (ol_delivery_d IS NOT NULL AND o_carrier_id IS NOT NULL)
/

-- Condition 8: W_YTD = sum(H_AMOUNT)
WITH x AS (SELECT w.w_id, w.w_ytd, h.sum_h_amount
           FROM warehouse w,
                (SELECT h_w_id, SUM(h_amount) sum_h_amount
                 FROM history
                 GROUP BY h_w_id) h
           WHERE w.w_id = h.h_w_id)
SELECT * FROM x
MINUS
SELECT * FROM x
WHERE w_ytd = sum_h_amount
/

-- Condition 9: D_YTD = sum(H_AMOUNT)
WITH x AS (SELECT d.d_w_id, d.d_id, d.d_ytd, h.sum_h_amount
           FROM district d,
                (SELECT h_w_id, h_d_id, SUM(h_amount) sum_h_amount
                 FROM history
                 GROUP BY h_w_id, h_d_id) h
           WHERE d.d_w_id = h.h_w_id AND
                 d.d_id = h.h_d_id)
SELECT * FROM x
MINUS
SELECT * FROM x
WHERE d_ytd = sum_h_amount
/

-- Condition 10: C_BALANCE = sum(OL_AMOUNT) - sum(H_AMOUNT)
WITH x AS (SELECT  c.c_w_id, c.c_d_id, c.c_id, c.c_balance,
                   o_ol.sum_ol_amount, h.sum_h_amount
           FROM customer c,
                (SELECT o.o_w_id, o.o_d_id, o.o_c_id,
                        SUM(ol_amount) sum_ol_amount
                 FROM orders o, order_line ol
                 WHERE o.o_w_id = ol.ol_w_id AND o.o_d_id = ol.ol_d_id AND
                       o.o_id = ol.ol_o_id AND ol.ol_delivery_d IS NOT NULL
                 GROUP BY o_w_id, o_d_id, o_c_id) o_ol,
                (SELECT h_c_w_id, h_c_d_id, h_c_id, SUM(h_amount) sum_h_amount
                 FROM history
                 GROUP BY h_c_w_id, h_c_d_id, h_c_id) h
           WHERE c.c_w_id = o_ol.o_w_id AND c.c_w_id = h.h_c_w_id AND
                 c.c_d_id = o_ol.o_d_id AND c.c_d_id = h.h_c_d_id AND
                 c.c_id = o_ol.o_c_id AND c.c_id = h.h_c_id)
SELECT * FROM x
MINUS
SELECT * FROM x
WHERE c_balance = sum_ol_amount - sum_h_amount
/

-- Condition 11: (count(*) from ORDER) - (count(*) from NEW-ORDER) = 2100
WITH x AS (SELECT o.o_w_id, o.o_d_id, o.count_o, no.count_no
           FROM (SELECT o_w_id, o_d_id, COUNT(*) count_o
                 FROM orders
                 GROUP BY o_w_id, o_d_id) o,
                (SELECT no_w_id, no_d_id, COUNT(*) count_no
                 FROM new_order
                 GROUP BY no_w_id, no_d_id) no
           WHERE o.o_w_id = no.no_w_id AND
                 o.o_d_id = no.no_d_id)
SELECT * FROM x
MINUS
SELECT * FROM x
WHERE count_o - count_no = 2100
/

-- Condition 12: C_BALANCE + C_YTD_PAYMENT = sum(OL_AMOUNT)
WITH x AS (SELECT c.c_w_id, c.c_d_id, c.c_id, c.c_balance, c.c_ytd_payment,
                  o_ol.sum_ol_amount
           FROM customer c,
                (SELECT o.o_w_id, o.o_d_id, o.o_c_id,
                        SUM(ol_amount) sum_ol_amount
                 FROM orders o, order_line ol
                 WHERE o.o_w_id = ol.ol_w_id AND o.o_d_id = ol.ol_d_id AND
                       o.o_id = ol.ol_o_id AND ol.ol_delivery_d IS NOT NULL
                 GROUP BY o_w_id, o_d_id, o_c_id) o_ol
           WHERE c.c_w_id = o_ol.o_w_id AND
                 c.c_d_id = o_ol.o_d_id AND
                 c.c_id = o_ol.o_c_id)
SELECT * FROM x
MINUS
SELECT * FROM x
WHERE c_balance + c_ytd_payment = sum_ol_amount
/
