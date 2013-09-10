-- ファクト表
-- 分毎のオーダー数(o_cnt)と金額合計(ol_amount)
-- o_cnt は分毎の New-Order トランザクション数(成功して commit した数)と同じはず
CREATE OR REPLACE VIEW orders_fact AS
  SELECT minute_id, to_char(o_w_id * 100 + o_d_id) district_id, o_cnt, sum(ol_amount) ol_amount
  FROM (SELECT TO_CHAR(TRUNC(o_entry_d, 'MI'), 'YYYYMMDDHH24MISS') minute_id,
               o_w_id, o_d_id, count(*) o_cnt
        FROM orders GROUP BY TRUNC(o_entry_d, 'MI'), o_w_id, o_d_id) o,
       order_line ol
  WHERE o.o_w_id = ol.ol_w_id AND o.o_d_id = ol.ol_d_id
  GROUP BY minute_id, o_w_id, o_d_id, o_cnt
/

-- district に関するディメンション
CREATE OR REPLACE VIEW district_dim AS
  SELECT '1' total_district_id,
         'Total District' total_district_dsc,
         TO_CHAR(d_w_id) warehouse_id,
         'W:' || TO_CHAR(d_w_id) || '|' || w_name warehouse_dsc,
         TO_CHAR(d_w_id * 100 + d_id) district_id,
         'W:' || TO_CHAR(d_w_id) || '|' || 'D:' || TO_CHAR(d_id) || '|' || d_name district_dsc
  FROM warehouse, district
  WHERE w_id = d_w_id
/

-- 時間に関するディメンション
-- 分 -> 15分 -> 時間
-- 最後のオーダーから180分前までの情報
-- 動的に変更されるため計算が多くややこしくなっている
CREATE OR REPLACE VIEW time_dim AS
  SELECT 'Total Hour' total_hour,
         TO_CHAR(TRUNC((max_minute - (i / (24 * 60))), 'HH24'), 'YYYYMMDDHH24') hour_id,
         60 * 60 hour_time_span,
         TRUNC((max_minute - (i / (24 * 60))), 'HH24') + (60 * 60 - 1) / (24 * 60 * 60) hour_end_date,
         TO_CHAR(FLOOR(TO_NUMBER(TO_CHAR(max_minute - (i / (24 * 60)), 'MI')) / 15) * 15) quarter,
         TO_CHAR((TRUNC(max_minute - (i / (24 * 60)), 'HH24') +
           (FLOOR(TO_NUMBER(TO_CHAR(max_minute - (i / (24 * 60)), 'MI')) / 15) * 15) / (24 * 60)),
           'YYYYMMDDHH24MI') quarter_id,
         15 * 60 quarter_time_span,
         TRUNC(max_minute - (i / (24 * 60)), 'HH24') +
           (FLOOR(TO_NUMBER(TO_CHAR(max_minute - (i / (24 * 60)), 'MI')) / 15) * 15) / (24 * 60) +
           (15 * 60 - 1) / (24 * 60 * 60) quarter_end_date,
         TO_CHAR(max_minute - (i / (24 * 60)), 'MI') minute,
         TO_CHAR(max_minute - (i / (24 * 60)), 'YYYYMMDDHH24MISS') minute_id,
         60 minute_time_span,
         max_minute - (i / (24 * 60)) + (59 / (24 * 60 * 60)) minute_end_date
  FROM (SELECT TRUNC(MAX(o_entry_d), 'MI') max_minute FROM orders) mm,
       (SELECT ROWNUM - 1 i FROM item WHERE ROWNUM <= 180) iterater
/
