# -*- coding: utf-8 -*-
require 'yaml'
require 'thread'
require 'java'
import java.sql.DriverManager
import Java::oracle.jdbc.driver.OracleDriver

require File.dirname(__FILE__) + '/helper'

class String
  def undent
    gsub(/^.{#{(slice(/^ +/) || '').length}}/, '')
  end
end

class TPCCSetup < RTA::Session
  include TPCCHelper

  INSERTS_PER_COMMIT = 100
  BATCH_SIZE = 100

  @@mutexes = [Mutex.new, Mutex.new]
  @@timestamp = java.sql.Date.new(Time.now.to_f * 1000)
  @@count_load = 0
  @@permutation = Array.new

  def setup_first
    unless config["setup"]["loading_only"]
      log.info("Create Table")
      # 接続
      con = get_connection

      last_warehouse_id = eval(config["warehouse_range"]).last
      create_table(con, config["setup"]["table_tablespace"],
                   config["setup"]["partition_count"], last_warehouse_id,
                   config["setup"]["parallel_degree"])

      con.close
    end

    log.info("TPCC Data Load Started...")
  end

  def setup
    # ログ
    # self.log = RTA::Log.new(TPCC_HOME + "/log/setup_#{@session_id}.log")
    # self.log.level = RTA::Log::DEBUG

    # config.yml の例
    # --
    # Configuration for load and tpcc script
    #
    # tpcc_user: tpcc
    # tpcc_password: tpcc
    # tpcc_url: jdbc:oracle:thin:@//192.168.1.5:1521/XE
    # warehouse_range: 1..10
    warehouse_range = eval(config["warehouse_range"])
    @first_warehouse_id = warehouse_range.first
    @last_warehouse_id =  warehouse_range.last
    if count_ware <= 0 || @first_warehouse_id <= 0 || @last_warehouse_id <= 0
      log.error("Invalid Warehouse Count.")
      exit(-1)
    end

    # 接続
    @con = get_connection

    @stmts = { :item => [], :ware => [], :stock => [], :district => [],
               :cust => [], :ord => [] }

    # トランザクション
    @tx_last = RTA::Transaction.new("last") do
      commit
    end

    self.go
  end

  def transaction
    tx = nil
    item_ld = MAXITEMS
    ware_ld = item_ld + count_ware
    stock_ld = ware_ld + (count_ware * MAXITEMS)
    district_ld = stock_ld + (count_ware * DIST_PER_WARE)
    cust_ld = district_ld + (count_ware * DIST_PER_WARE * CUST_PER_DIST)
    ord_ld = cust_ld + (count_ware * DIST_PER_WARE * ORD_PER_DIST)

    @@mutexes[0].synchronize do
      @@count_load += 1
      if @@count_load <= item_ld # Loads the Item table
        @loading = @@count_load
        log.info("Loading Item") if @loading == 1
        tx = load_items
      elsif @@count_load <= ware_ld # Loads the Warehouse table
        @loading = @@count_load - item_ld
        log.info("Loading Warehouse") if @loading == 1
        tx = load_ware
      elsif @@count_load <= stock_ld # Loads the Stock table
        @loading = @@count_load - ware_ld
        @w_id = ((@loading - 1) / MAXITEMS) + @first_warehouse_id
        log.info("Loading Stock Wid=#{@w_id}") if (@loading % MAXITEMS) == 1
        tx = load_stock
      elsif @@count_load <= district_ld # Loads the District table
        @loading = @@count_load - stock_ld
        log.info("Loading District") if @loading == 1
        @w_id = ((@loading - 1) / DIST_PER_WARE) + @first_warehouse_id
        tx = load_district
      elsif @@count_load <= cust_ld # Loads the Customer table
        @loading = @@count_load - district_ld
        @w_id = ((@loading - 1) / (DIST_PER_WARE * CUST_PER_DIST)) + @first_warehouse_id
        @d_id = ((@loading - 1) / CUST_PER_DIST) % DIST_PER_WARE + 1

        log.info("Loading Customer for DID=#{@d_id}, WID=#{@w_id}") if (@loading % CUST_PER_DIST) == 1
        tx = load_cust
      elsif @@count_load <= ord_ld # Loads the Orders and Order-Line tables
        @loading = @@count_load - cust_ld
        @w_id = ((@loading - 1) / (DIST_PER_WARE * ORD_PER_DIST)) + @first_warehouse_id
        @d_id = ((@loading - 1) / ORD_PER_DIST) % DIST_PER_WARE + 1

        log.info("Loading Orders for D=#{@d_id}, W=#{@w_id}") if (@loading % ORD_PER_DIST) == 1
        init_permutation if @loading % 3000 == 1
        tx = load_ord
      else
        stop
        tx = @tx_last
      end
    end
    return tx
  end

  def teardown
    commit
    @con.close
  end

  def teardown_last
    log.info("...DATA LOADING COMPLETED SUCCESSFULLY.")
    return if config["setup"]["loading_only"]

    # 接続
    con = get_connection

    log.info("Create Index")
    create_index(con, config["setup"]["index_tablespace"],
                 config["setup"]["partition_count"],
                 config["setup"]["parallel_degree"])

    log.info("Add Constraint")
    add_constraint(con)

    log.info("Validate Constraint")
    validate_constraint(con)

    log.info("Analyze")
    analyze(con, config["setup"]["parallel_degree"])

    con.close
  end

  def load_items
    unless @load_items
      @load_items = RTA::Transaction.new("load items") do
        begin
          i_id = @loading
          # I_IM_ID random within [1 .. 10,000]
          i_im_id = random_number(1, 10000)
          i_name = make_alpha_string(14, 24)
          i_price = random_number(100, 10000) / 100.0
          i_data = make_alpha_string(26, 50)
          insert_original!(i_data) if rand(10) == 0

          log.debug("IID: #{i_id}, Name: #{i_name}, Price: #{i_price}")

          sql = "INSERT INTO item (i_id, i_im_id, i_name, i_price, i_data) "+
                "VALUES (?, ?, ?, ?, ?)"
          @stmts[:item][0] ||= @con.prepareStatement(sql)
          stmt = @stmts[:item][0]
          stmt.setInt(1, i_id)
          stmt.setInt(2, i_im_id)
          stmt.setString(3, i_name)
          stmt.setDouble(4, i_price)
          stmt.setString(5, i_data)
          if BATCH_SIZE > 0
            stmt.addBatch
          else
            stmt.executeUpdate
          end

          log.info(i_id.to_s) if i_id % 5000 == 0
        ensure
          unless BATCH_SIZE > 0
            @stmts[:item].each { |st| st.close if st }
            @stmts[:item] = []
          end
        end
      end

      @load_items.after_each do
        execute_batch if BATCH_SIZE > 0 && self.stat.count % BATCH_SIZE == 0

        commit if self.stat.count % INSERTS_PER_COMMIT == 0

        if @loading == MAXITEMS
          commit
          log.info("Item Done.")
        end
      end

      @load_items.whenever_sqlerror { @con.rollback }
    end
    return @load_items
  end

  def load_ware
    unless @load_ware
      @load_ware = RTA::Transaction.new("load ware") do
        begin
          w_id = @loading + @first_warehouse_id - 1
          w_name = make_alpha_string(6, 10)
          w_street_1, w_street_2, w_city, w_state, w_zip = make_address
          w_tax = random_number(0, 20) / 100.0 # W_TAX random within [0.0000 .. 0.2000]
          w_ytd = 300000.00 # W_YTD = 300,000.00

          log.debug("WID: #{w_id}, Name: #{w_name}, Tax: #{w_tax}")

          sql = "INSERT INTO warehouse (w_id, w_name, " +
                "w_street_1, w_street_2, w_city, w_state, w_zip, w_tax, w_ytd) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)"
          @stmts[:ware][0] ||= @con.prepareStatement(sql)
          stmt = @stmts[:ware][0]
          stmt.setInt(1, w_id)
          stmt.setString(2, w_name)
          stmt.setString(3, w_street_1)
          stmt.setString(4, w_street_2)
          stmt.setString(5, w_city)
          stmt.setString(6, w_state)
          stmt.setString(7, w_zip)
          stmt.setDouble(8, w_tax)
          stmt.setDouble(9, w_ytd)
          if BATCH_SIZE > 0
            stmt.addBatch
          else
            stmt.executeUpdate
          end
        ensure
          unless BATCH_SIZE > 0
            @stmts[:ware].each { |st| st.close if st }
            @stmts[:ware] = []
          end
        end
      end

      @load_ware.after_each do
        execute_batch if BATCH_SIZE > 0 && self.stat.count % BATCH_SIZE == 0

        commit if self.stat.count % INSERTS_PER_COMMIT == 0
        commit if @loading == count_ware
      end

      @load_ware.whenever_sqlerror { @con.rollback }
    end
    return @load_ware
  end

  def load_stock
    unless @load_stock
      @load_stock = RTA::Transaction.new("load stock") do
        begin
          s_w_id = @w_id
          s_i_id = (@loading - 1) % MAXITEMS + 1
          s_quantity = random_number(10, 100)
          s_dist_01 = make_alpha_string(24, 24)
          s_dist_02 = make_alpha_string(24, 24)
          s_dist_03 = make_alpha_string(24, 24)
          s_dist_04 = make_alpha_string(24, 24)
          s_dist_05 = make_alpha_string(24, 24)
          s_dist_06 = make_alpha_string(24, 24)
          s_dist_07 = make_alpha_string(24, 24)
          s_dist_08 = make_alpha_string(24, 24)
          s_dist_09 = make_alpha_string(24, 24)
          s_dist_10 = make_alpha_string(24, 24)
          s_data = make_alpha_string(26, 50)
          insert_original!(s_data) if rand(10) == 0

          log.debug("SID: #{s_i_id}, WID: #{s_w_id}, Quan: #{s_quantity}")

          sql = "INSERT INTO stock (s_i_id, s_w_id, s_quantity, " +
                "s_dist_01, s_dist_02, s_dist_03, s_dist_04, s_dist_05, " +
                "s_dist_06, s_dist_07, s_dist_08, s_dist_09, s_dist_10, " +
                "s_data, s_ytd, s_order_cnt, s_remote_cnt) " +
                "VALUES (?, ?, ?, " +
                "?, ?, ?, ?, ?, " +
                "?, ?, ?, ?, ?, " +
                "?, 0, 0, 0)"
          @stmts[:stock][0] ||= @con.prepareStatement(sql)
          stmt = @stmts[:stock][0]
          stmt.setInt(1, s_i_id)
          stmt.setInt(2, s_w_id)
          stmt.setInt(3, s_quantity)
          stmt.setString(4, s_dist_01)
          stmt.setString(5, s_dist_02)
          stmt.setString(6, s_dist_03)
          stmt.setString(7, s_dist_04)
          stmt.setString(8, s_dist_05)
          stmt.setString(9, s_dist_06)
          stmt.setString(10, s_dist_07)
          stmt.setString(11, s_dist_08)
          stmt.setString(12, s_dist_09)
          stmt.setString(13, s_dist_10)
          stmt.setString(14, s_data)
          if BATCH_SIZE > 0
            stmt.addBatch
          else
            stmt.executeUpdate
          end

          log.info(s_i_id.to_s) if s_i_id % 5000 == 0
        ensure
          unless BATCH_SIZE > 0
            @stmts[:stock].each { |st| st.close if st }
            @stmts[:stock] = []
          end
        end
      end

      @load_stock.after_each do
        execute_batch if BATCH_SIZE > 0 && self.stat.count % BATCH_SIZE == 0

        commit if self.stat.count % INSERTS_PER_COMMIT == 0

        if @loading == count_ware * MAXITEMS
          commit
          log.info("Stock Done.")
        end
      end

      @load_stock.whenever_sqlerror { @con.rollback }
    end
    return @load_stock
  end

  def load_district
    unless @load_district
      @load_district = RTA::Transaction.new("load district") do
        begin
          d_w_id = @w_id
          d_ytd = 30000.0
          d_next_o_id = 3001
          d_id = (@loading - 1) % DIST_PER_WARE + 1
          d_name = make_alpha_string(6, 10)
          d_street_1, d_street_2, d_city, d_state, d_zip = make_address
          d_tax = random_number(0, 20) / 100.0 # D_TAX random within [0.0000 .. 0.2000]

          log.debug("DID: #{d_id}, WID: #{d_w_id}, Name: #{d_name}, Tax: #{d_tax}")

          sql = "INSERT INTO district (d_id, d_w_id, d_name, " +
                "d_street_1, d_street_2, d_city, d_state, d_zip, " +
                "d_tax, d_ytd, d_next_o_id) " +
                "VALUES (?, ?, ?, " +
                "?, ?, ?, ?, ?, " +
                "?, ?, ?)"
          @stmts[:district][0] ||= @con.prepareStatement(sql)
          stmt = @stmts[:district][0]
          stmt.setInt(1, d_id)
          stmt.setInt(2, d_w_id)
          stmt.setString(3, d_name)
          stmt.setString(4, d_street_1)
          stmt.setString(5, d_street_2)
          stmt.setString(6, d_city)
          stmt.setString(7, d_state)
          stmt.setString(8, d_zip)
          stmt.setDouble(9, d_tax)
          stmt.setDouble(10, d_ytd)
          stmt.setInt(11, d_next_o_id)
          if BATCH_SIZE > 0
            stmt.addBatch
          else
            stmt.executeUpdate
          end
        ensure
          unless BATCH_SIZE > 0
            @stmts[:district].each { |st| st.close if st }
            @stmts[:district] = []
          end
        end
      end

      @load_district.after_each do
        execute_batch if BATCH_SIZE > 0 && self.stat.count % BATCH_SIZE == 0

        commit if self.stat.count % INSERTS_PER_COMMIT == 0
        commit if @loading == count_ware * DIST_PER_WARE
      end

      @load_district.whenever_sqlerror { @con.rollback }
    end
    return @load_district
  end

  def load_cust
    unless @load_cust
      @load_cust = RTA::Transaction.new("load cust") do
        begin
          stmt = [nil, nil]
          c_id = (@loading - 1) % CUST_PER_DIST + 1
          c_d_id = @d_id
          c_w_id = @w_id
          c_first = make_alpha_string(8, 16)
          c_middle = "OE"
          if c_id <= 1000
            c_last = lastname(c_id - 1)
          else
            c_last = lastname(nurand(255, 0, 999))
          end
          c_street_1, c_street_2, c_city, c_state, c_zip = make_address
          c_phone = make_number_string(16, 16)
          # C_CREDIT = "GC". For 10% of the rows, selected at random , C_CREDIT = "BC"
          unless random_number(1, 10) == 1
            c_credit = 'G'
          else
            c_credit = 'B'
          end
          c_credit = c_credit + 'C'
          c_credit_lim = 50000.00
          c_discount = random_number(0, 50) / 100.0
          c_balance = -10.0
          c_data = make_alpha_string(300, 500)

          log.debug("CID: #{c_id}, LST: #{c_last}, P#: #{c_phone}")

          sql = "INSERT INTO customer (c_id, c_d_id, c_w_id, " +
                "c_first, c_middle, c_last, " +
                "c_street_1, c_street_2, c_city, c_state, c_zip, " +
                "c_phone, c_since, c_credit, " +
                "c_credit_lim, c_discount, c_balance, c_data, " +
                "c_ytd_payment, c_payment_cnt, c_delivery_cnt) " +
                "VALUES (?, ?, ?, " +
                "?, ?, ?, " +
                "?, ?, ?, ?, ?, " +
                "?, ?, ?, " +
                "?, ?, ?, ?, " +
                "10.0, 1, 0)"
          @stmts[:cust][0] ||= @con.prepareStatement(sql)
          stmt[0] = @stmts[:cust][0]
          stmt[0].setInt(1, c_id)
          stmt[0].setInt(2, c_d_id)
          stmt[0].setInt(3, c_w_id)
          stmt[0].setString(4, c_first)
          stmt[0].setString(5, c_middle)
          stmt[0].setString(6, c_last)
          stmt[0].setString(7, c_street_1)
          stmt[0].setString(8, c_street_2)
          stmt[0].setString(9, c_city)
          stmt[0].setString(10, c_state)
          stmt[0].setString(11, c_zip)
          stmt[0].setString(12, c_phone)
          stmt[0].setDate(13, @@timestamp)
          stmt[0].setString(14, c_credit)
          stmt[0].setDouble(15, c_credit_lim)
          stmt[0].setDouble(16, c_discount)
          stmt[0].setDouble(17, c_balance)
          stmt[0].setString(18, c_data)
          if BATCH_SIZE > 0
            stmt[0].addBatch
          else
            stmt[0].executeUpdate
          end

          h_amount = 10.0
          h_data = make_alpha_string(12, 24)

          sql = "INSERT INTO history (h_c_id, h_c_d_id, h_c_w_id, " +
                "h_w_id, h_d_id, h_date, h_amount, h_data) " +
                "VALUES (?, ?, ?, " +
                "?, ?, ?, ?, ?)"
          @stmts[:cust][1] ||= @con.prepareStatement(sql)
          stmt[1] = @stmts[:cust][1]
          stmt[1].setInt(1, c_id)
          stmt[1].setInt(2, c_d_id)
          stmt[1].setInt(3, c_w_id)
          stmt[1].setInt(4, c_w_id)
          stmt[1].setInt(5, c_d_id)
          stmt[1].setDate(6, @@timestamp)
          stmt[1].setDouble(7, h_amount)
          stmt[1].setString(8, h_data)
          if BATCH_SIZE > 0
            stmt[1].addBatch
          else
            stmt[1].executeUpdate
          end

          log.info(c_id.to_s) if c_id % 1000 == 0
        ensure
          unless BATCH_SIZE > 0
            @stmts[:cust].each { |st| st.close if st }
            @stmts[:cust] = []
          end
        end
      end

      @load_cust.after_each do
        execute_batch if BATCH_SIZE > 0 && self.stat.count % BATCH_SIZE == 0

        commit if self.stat.count % INSERTS_PER_COMMIT == 0

        if @loading == count_ware * DIST_PER_WARE * CUST_PER_DIST
          commit
          log.info("Customer Done.")
        end
      end

      @load_cust.whenever_sqlerror { @con.rollback }
    end
    return @load_cust
  end

  def load_ord
    unless @load_ord
      @load_ord = RTA::Transaction.new("load ord") do
        begin
          stmt = [nil, nil, nil, nil, nil]
          o_d_id = @d_id
          o_w_id = @w_id
          o_id = (@loading - 1) % ORD_PER_DIST + 1
          o_c_id = get_permutation
          o_carrier_id = random_number(1, 10)
          o_ol_cnt = random_number(5, 15)

          log.debug("OID: #{o_id}, CID: #{o_c_id}, DID: #{o_d_id}, WID: #{o_w_id}")

          if o_id > 2100 # the last 900 orders have not been delivered
            sql = "INSERT INTO orders (o_id, o_c_id, o_d_id, o_w_id, " +
                  "o_entry_d, o_carrier_id, o_ol_cnt, o_all_local) " +
                  "VALUES (?, ?, ?, ?, " +
                  "?, NULL, ?, 1)"
            @stmts[:ord][0] ||= @con.prepareStatement(sql)
            stmt[0] = @stmts[:ord][0]
            stmt[0].setInt(1, o_id)
            stmt[0].setInt(2, o_c_id)
            stmt[0].setInt(3, o_d_id)
            stmt[0].setInt(4, o_w_id)
            stmt[0].setDate(5, @@timestamp)
            stmt[0].setInt(6, o_ol_cnt)
            if BATCH_SIZE > 0
              stmt[0].addBatch
            else
              stmt[0].executeUpdate
            end

            sql = "INSERT INTO new_order (no_o_id, no_d_id, no_w_id) " +
                "VALUES (?, ?, ?)"
            @stmts[:ord][1] ||= @con.prepareStatement(sql)
            stmt[1] = @stmts[:ord][1]
            stmt[1].setInt(1, o_id)
            stmt[1].setInt(2, o_d_id)
            stmt[1].setInt(3, o_w_id)
            if BATCH_SIZE > 0
              stmt[1].addBatch
            else
              stmt[1].executeUpdate
            end
          else
            sql = "INSERT INTO orders (o_id, o_c_id, o_d_id, o_w_id, " +
                  "o_entry_d, o_carrier_id, o_ol_cnt, o_all_local) " +
                  "VALUES (?, ?, ?, ?, " +
                  "?, ?, ?, 1)"
            @stmts[:ord][2] ||= @con.prepareStatement(sql)
            stmt[2] = @stmts[:ord][2]
            stmt[2].setInt(1, o_id)
            stmt[2].setInt(2, o_c_id)
            stmt[2].setInt(3, o_d_id)
            stmt[2].setInt(4, o_w_id)
            stmt[2].setDate(5, @@timestamp)
            stmt[2].setInt(6, o_carrier_id)
            stmt[2].setInt(7, o_ol_cnt)
            if BATCH_SIZE > 0
              stmt[2].addBatch
            else
              stmt[2].executeUpdate
            end
          end

          1.upto(o_ol_cnt) do |ol|
            ol_i_id = random_number(1, MAXITEMS)
            ol_supply_w_id = o_w_id
            ol_quantity = 5
            ol_amount = 0.0
            ol_dist_info = make_alpha_string(24, 24)

            if o_id > 2100
              # OL_AMOUNT = 0.00 if OL_O_ID < 2,101, random within [0.01 .. 9,999.99] otherwise
              ol_amount = random_number(1, 999999) / 100.0

              log.debug("OL: #{ol}, IID: #{ol_i_id}, QUAN: #{ol_quantity}, AMT: #{ol_amount}")

              sql = "INSERT INTO order_line (ol_o_id, ol_d_id, ol_w_id, ol_number, " +
                    "ol_i_id, ol_supply_w_id, ol_quantity, ol_amount, " +
                    "ol_dist_info, ol_delivery_d) " +
                    "VALUES (?, ?, ?, ?, " +
                    "?, ?, ?, ?, " +
                    "?, NULL)"
              @stmts[:ord][3] ||= @con.prepareStatement(sql)
              stmt[3] = @stmts[:ord][3]
              stmt[3].setInt(1, o_id)
              stmt[3].setInt(2, o_d_id)
              stmt[3].setInt(3, o_w_id)
              stmt[3].setInt(4, ol)
              stmt[3].setInt(5, ol_i_id)
              stmt[3].setInt(6, ol_supply_w_id)
              stmt[3].setInt(7, ol_quantity)
              stmt[3].setDouble(8, ol_amount)
              stmt[3].setString(9, ol_dist_info)
              if BATCH_SIZE > 0
                stmt[3].addBatch
              else
                stmt[3].executeUpdate
              end
            else
              # OL_AMOUNT = 0.00 if OL_O_ID < 2,101, random within [0.01 .. 9,999.99] otherwise
              ol_amount = 0.0

              log.debug("OL: #{ol}, IID: #{ol_i_id}, QUAN: #{ol_quantity}, AMT: #{ol_amount}")

              sql = "INSERT INTO order_line (ol_o_id, ol_d_id, ol_w_id, ol_number, " +
                    "ol_i_id, ol_supply_w_id, ol_quantity, ol_amount, " +
                    "ol_dist_info, ol_delivery_d) " +
                    "VALUES (?, ?, ?, ?, " +
                    "?, ?, ?, ?, " +
                    "?, ?)"
              @stmts[:ord][4] ||= @con.prepareStatement(sql)
              stmt[4] = @stmts[:ord][4]
              stmt[4].setInt(1, o_id)
              stmt[4].setInt(2, o_d_id)
              stmt[4].setInt(3, o_w_id)
              stmt[4].setInt(4, ol)
              stmt[4].setInt(5, ol_i_id)
              stmt[4].setInt(6, ol_supply_w_id)
              stmt[4].setInt(7, ol_quantity)
              stmt[4].setDouble(8, ol_amount)
              stmt[4].setString(9, ol_dist_info)
              stmt[4].setDate(10, @@timestamp)
              if BATCH_SIZE > 0
                stmt[4].addBatch
              else
                stmt[4].executeUpdate
              end
            end
          end

          log.info(o_id.to_s) if o_id % 1000 == 0
        ensure
          unless BATCH_SIZE > 0
            @stmts[:ord].each { |st| st.close if st }
            @stmts[:ord] = []
          end
        end
      end

      @load_ord.after_each do
        execute_batch if BATCH_SIZE > 0 && self.stat.count % BATCH_SIZE == 0

        commit if self.stat.count % INSERTS_PER_COMMIT == 0

        if @loading == count_ware * DIST_PER_WARE * ORD_PER_DIST
          commit
          log.info("Orders Done.")
        end
      end

      @load_ord.whenever_sqlerror { @con.rollback }
    end
    return @load_ord
  end

  def make_alpha_string(min, max)
    charset = ('a'..'z').to_a + ('A'..'Z').to_a + ('0'..'9').to_a
    return make_random_string(min, max, charset)
  end

  def make_number_string(min, max)
    charset = ('0'..'9').to_a
    return make_random_string(min, max, charset)
  end

  def make_random_string(min, max, charset)
    return Array.new(random_number(min, max)) { charset[rand(charset.size)] }.join
  end

  def make_address
    # The warehouse zip code (W_ZIP), the district zip code (D_ZIP) and the
    # customer zip code (C_ZIP) must be generated by the concatenation of:
    #   1. A random n-string of 4 numbers, and
    #   2. The constant string '11111'.
    # Given a random n-string between 0 and 9999, the zip codes are determined
    # by concatenating the n-string and the constant '11111'. This will create
    # 10,000 unique zip codes. For example, the n-string 0503 concatenated with
    # 11111, will make the zip code 050311111
    return make_alpha_string(10, 20), make_alpha_string(10, 20), make_alpha_string(10, 20), make_alpha_string(2, 2), make_number_string(4, 4) + '11111'
  end

  def insert_original!(str)
    pos = random_number(0, str.size - "original".size)
    str[pos, "original".size] = "original"
  end

  def init_permutation
    ary = Array(1..3000)
    ary.each_index do |i|
      j = rand(i + 1)
      ary[i], ary[j] = ary[j], ary[i]
    end
    until @@permutation.size == 0
      sleep 0.01
    end
    @@permutation = ary
  end

  def get_permutation
    ret = nil
    @@mutexes[1].synchronize do
      ret = @@permutation.shift
    end
    raise "permutation is nil" if ret.nil?
    return ret
  end

  def count_ware
    return @last_warehouse_id - @first_warehouse_id + 1
  end

  def commit
    execute_batch if BATCH_SIZE > 0
    @con.commit
  end

  def execute_batch
    @stmts.each_value do |sts|
      sts.each do |stmt|
        if stmt
          stmt.executeBatch
          stmt.close
        end
      end
    end
    @stmts = { :item => [], :ware => [], :stock => [], :district => [],
               :cust => [], :ord => [] }
  end

  private
  def config
    if @config.nil?
      @config = Hash.new
      File.open(TPCC_HOME + '/config/config.yml', 'r') do |file|
        @config = YAML.load(file.read)
      end
    end
    return @config
  end

  def get_connection
    # 接続
    begin
      con = DriverManager.getConnection(config["tpcc_url"],
              config["tpcc_user"], config["tpcc_password"])
      con.setAutoCommit(false)
    rescue SQLException => e
      e.printStackTrace
    rescue ClassNotFoundException => e
      e.printStackTrace
    end
    return con
  end

  def create_table(con, tablespace_name, partition_count, last_warehouse_id, parallel_degree)
    sqls = Array.new

    # warehouse
    sql = <<-EOS
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
      TABLESPACE #{tablespace_name}
      PARALLEL #{parallel_degree}
    EOS
    sql += table_partitioning_clause("w_id", last_warehouse_id,
                                     partition_count, tablespace_name)
    sqls << sql

    # district
    sql = <<-EOS
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
      TABLESPACE #{tablespace_name}
      PARALLEL #{parallel_degree}
    EOS
    sql += table_partitioning_clause("d_w_id", last_warehouse_id,
                                     partition_count, tablespace_name)
    sqls << sql

    # customer
    sql = <<-EOS
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
      TABLESPACE #{tablespace_name}
      PARALLEL #{parallel_degree}
    EOS
    sql += table_partitioning_clause("c_w_id", last_warehouse_id,
                                     partition_count, tablespace_name)
    sqls << sql

    # history
    sql = <<-EOS
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
      TABLESPACE #{tablespace_name}
      PARALLEL #{parallel_degree}
    EOS
    sql += table_partitioning_clause("h_w_id", last_warehouse_id,
                                     partition_count, tablespace_name)
    sqls << sql

    # new_order
    sql = <<-EOS
      CREATE TABLE new_order
      (
        no_o_id NUMBER(8, 0), -- 10,000,000 unique IDs
        no_d_id NUMBER(2, 0), -- 20 unique IDs
        no_w_id NUMBER        -- 2*W unique IDs
      )
      TABLESPACE #{tablespace_name}
      PARALLEL #{parallel_degree}
    EOS
    sql += table_partitioning_clause("no_w_id", last_warehouse_id,
                                     partition_count, tablespace_name)
    sqls << sql

    # orders
    sql = <<-EOS
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
      TABLESPACE #{tablespace_name}
      PARALLEL #{parallel_degree}
    EOS
    sql += table_partitioning_clause("o_w_id", last_warehouse_id,
                                     partition_count, tablespace_name)
    sqls << sql

    # order_line
    sql = <<-EOS
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
      TABLESPACE #{tablespace_name}
      PARALLEL #{parallel_degree}
    EOS
    sql += table_partitioning_clause("ol_w_id", last_warehouse_id,
                                     partition_count, tablespace_name)
    sqls << sql

    # item
    sql = <<-EOS
      CREATE TABLE item
      (
        i_id    NUMBER(6, 0), -- 200,000 unique IDs
        i_im_id NUMBER(6, 0), -- 200,000 unique IDs
        i_name  VARCHAR2(24),
        i_price NUMBER(5, 2),
        i_data  VARCHAR2(50)
      )
      TABLESPACE #{tablespace_name}
      PARALLEL #{parallel_degree}
    EOS
    sqls << sql

    # stock
    sql = <<-EOS
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
        s_order_cnt  NUMBER,
        s_remote_cnt NUMBER,
        s_data       VARCHAR2(50)
      )
      TABLESPACE #{tablespace_name}
      PARALLEL #{parallel_degree}
    EOS
    sql += table_partitioning_clause("s_w_id", last_warehouse_id,
                                     partition_count, tablespace_name)
    sqls << sql

    sqls.each { |sql| exec_sql(con, sql.chomp.undent) }
  end

  def create_index(con, tablespace_name, partition_count, parallel_degree)
    sqls = Array.new

    # warehouse_pk
    sql = <<-EOS
      CREATE UNIQUE INDEX warehouse_pk
        ON warehouse ( w_id )
    EOS
    sql += index_partitioning_clause(partition_count)
    sql += <<-EOS
      TABLESPACE #{tablespace_name}
      PARALLEL #{parallel_degree}
    EOS
    sqls << sql

    # district_pk
    sql = <<-EOS
      CREATE UNIQUE INDEX district_pk
        ON district ( d_w_id, d_id )
    EOS
    sql += index_partitioning_clause(partition_count)
    sql += <<-EOS
      TABLESPACE #{tablespace_name}
      PARALLEL #{parallel_degree}
    EOS
    sqls << sql

    # customer_pk
    sql = <<-EOS
      CREATE UNIQUE INDEX customer_pk
        ON customer ( c_w_id, c_d_id, c_id )
    EOS
    sql += index_partitioning_clause(partition_count)
    sql += <<-EOS
      TABLESPACE #{tablespace_name}
      PARALLEL #{parallel_degree}
    EOS
    sqls << sql

    # new_order_pk
    sql = <<-EOS
      CREATE UNIQUE INDEX new_order_pk
        ON new_order ( no_w_id, no_d_id, no_o_id )
    EOS
    sql += index_partitioning_clause(partition_count)
    sql += <<-EOS
      TABLESPACE #{tablespace_name}
      PARALLEL #{parallel_degree}
    EOS
    sqls << sql

    # orders_pk
    sql = <<-EOS
      CREATE UNIQUE INDEX orders_pk
        ON orders ( o_w_id, o_d_id, o_id )
    EOS
    sql += index_partitioning_clause(partition_count)
    sql += <<-EOS
      TABLESPACE #{tablespace_name}
      PARALLEL #{parallel_degree}
    EOS
    sqls << sql

    # order_line_pk
    sql = <<-EOS
      CREATE UNIQUE INDEX order_line_pk
        ON order_line ( ol_w_id, ol_d_id, ol_o_id, ol_number )
    EOS
    sql += index_partitioning_clause(partition_count)
    sql += <<-EOS
      TABLESPACE #{tablespace_name}
      PARALLEL #{parallel_degree}
    EOS
    sqls << sql

    # item_pk
    sql = <<-EOS
      CREATE UNIQUE INDEX item_pk
        ON item ( i_id )
      TABLESPACE #{tablespace_name}
      PARALLEL #{parallel_degree}
    EOS
    sqls << sql

    # stock_pk
    sql = <<-EOS
      CREATE UNIQUE INDEX stock_pk
        ON stock ( s_w_id, s_i_id )
    EOS
    sql += index_partitioning_clause(partition_count)
    sql += <<-EOS
      TABLESPACE #{tablespace_name}
      PARALLEL #{parallel_degree}
    EOS
    sqls << sql

    # Additional index
    # customer_idx
    sql = <<-EOS
      CREATE UNIQUE INDEX customer_idx
        ON customer ( c_last, c_w_id, c_d_id, c_first, c_id )
    EOS
    sql += index_partitioning_clause(partition_count)
    sql += <<-EOS
      TABLESPACE #{tablespace_name}
      PARALLEL #{parallel_degree}
    EOS
    sqls << sql

    # orders_idx
    sql = <<-EOS
      CREATE UNIQUE INDEX orders_idx
        ON orders ( o_c_id, o_d_id, o_w_id, o_id )
    EOS
    sql += index_partitioning_clause(partition_count)
    sql += <<-EOS
      TABLESPACE #{tablespace_name}
      PARALLEL #{parallel_degree}
    EOS
    sqls << sql

    sqls.each { |sql| exec_sql(con, sql.chomp.undent) }
  end

  def add_constraint(con)
    sqls = Array.new

    # Primary key
    sql = <<-EOS
      ALTER TABLE warehouse ADD CONSTRAINT warehouse_pk
        PRIMARY KEY ( w_id )
        USING INDEX warehouse_pk
        ENABLE NOVALIDATE
    EOS
    sqls << sql

    sql = <<-EOS
      ALTER TABLE district ADD CONSTRAINT district_pk
        PRIMARY KEY ( d_w_id, d_id )
        USING INDEX district_pk
        ENABLE NOVALIDATE
    EOS
    sqls << sql

    sql = <<-EOS
      ALTER TABLE customer ADD CONSTRAINT customer_pk
        PRIMARY KEY ( c_w_id, c_d_id, c_id )
        USING INDEX customer_pk
        ENABLE NOVALIDATE
    EOS
    sqls << sql

    sql = <<-EOS
      ALTER TABLE new_order ADD CONSTRAINT new_order_pk
        PRIMARY KEY ( no_w_id, no_d_id, no_o_id )
        USING INDEX new_order_pk
        ENABLE NOVALIDATE
    EOS
    sqls << sql

    sql = <<-EOS
      ALTER TABLE orders ADD CONSTRAINT orders_pk
        PRIMARY KEY ( o_w_id, o_d_id, o_id )
        USING INDEX orders_pk
        ENABLE NOVALIDATE
    EOS
    sqls << sql

    sql = <<-EOS
      ALTER TABLE order_line ADD CONSTRAINT order_line_pk
        PRIMARY KEY ( ol_w_id, ol_d_id, ol_o_id, ol_number )
        USING INDEX order_line_pk
        ENABLE NOVALIDATE
    EOS
    sqls << sql

    sql = <<-EOS
      ALTER TABLE item ADD CONSTRAINT item_pk
        PRIMARY KEY ( i_id )
        USING INDEX item_pk
        ENABLE NOVALIDATE
    EOS
    sqls << sql

    sql = <<-EOS
      ALTER TABLE stock ADD CONSTRAINT stock_pk
        PRIMARY KEY ( s_w_id, s_i_id )
        USING INDEX stock_pk
        ENABLE NOVALIDATE
    EOS
    sqls << sql

    # Foreign key
    sql = <<-EOS
      ALTER TABLE district ADD CONSTRAINT district_fk
        FOREIGN KEY ( d_w_id )
        REFERENCES warehouse ( w_id )
        ENABLE NOVALIDATE
    EOS
    sqls << sql

    sql = <<-EOS
      ALTER TABLE customer ADD CONSTRAINT customer_fk
        FOREIGN KEY ( c_w_id, c_d_id )
        REFERENCES district ( d_w_id, d_id )
        ENABLE NOVALIDATE
    EOS
    sqls << sql

    sql = <<-EOS
      ALTER TABLE history ADD CONSTRAINT history_fk1
        FOREIGN KEY ( h_c_w_id, h_c_d_id, h_c_id )
        REFERENCES customer ( c_w_id, c_d_id, c_id )
        ENABLE NOVALIDATE
    EOS
    sqls << sql

    sql = <<-EOS
      ALTER TABLE history ADD CONSTRAINT history_fk2
        FOREIGN KEY ( h_w_id, h_d_id )
        REFERENCES district ( d_w_id, d_id )
        ENABLE NOVALIDATE
    EOS
    sqls << sql

    sql = <<-EOS
      ALTER TABLE new_order ADD CONSTRAINT new_order_fk
        FOREIGN KEY ( no_w_id, no_d_id, no_o_id )
        REFERENCES orders ( o_w_id, o_d_id, o_id )
        ENABLE NOVALIDATE
    EOS
    sqls << sql

    sql = <<-EOS
      ALTER TABLE orders ADD CONSTRAINT orders_fk
        FOREIGN KEY ( o_w_id, o_d_id, o_c_id )
        REFERENCES customer ( c_w_id, c_d_id, c_id )
        ENABLE NOVALIDATE
    EOS
    sqls << sql

    sql = <<-EOS
      ALTER TABLE order_line ADD CONSTRAINT order_line_fk1
        FOREIGN KEY ( ol_w_id, ol_d_id, ol_o_id )
        REFERENCES orders ( o_w_id, o_d_id, o_id )
        ENABLE NOVALIDATE
    EOS
    sqls << sql

    sql = <<-EOS
      ALTER TABLE order_line ADD CONSTRAINT order_line_fk2
        FOREIGN KEY ( ol_supply_w_id, ol_i_id )
        REFERENCES stock ( s_w_id, s_i_id )
        ENABLE NOVALIDATE
    EOS
    sqls << sql

    sql = <<-EOS
      ALTER TABLE stock ADD CONSTRAINT stock_fk1
        FOREIGN KEY ( s_w_id )
        REFERENCES warehouse ( w_id )
        ENABLE NOVALIDATE
    EOS
    sqls << sql

    sql = <<-EOS
      ALTER TABLE stock ADD CONSTRAINT stock_fk2
        FOREIGN KEY ( s_i_id )
        REFERENCES item ( i_id )
        ENABLE NOVALIDATE
    EOS
    sqls << sql

    sqls.each { |sql| exec_sql(con, sql.chomp.undent) }
  end

  def validate_constraint(con)
    sqls = Array.new

    # Primary key
    sql = <<-EOS
      ALTER TABLE district
      MODIFY CONSTRAINT district_pk VALIDATE
    EOS
    sqls << sql

    sql = <<-EOS
      ALTER TABLE customer
      MODIFY CONSTRAINT customer_pk VALIDATE
    EOS
    sqls << sql

    sql = <<-EOS
      ALTER TABLE new_order
      MODIFY CONSTRAINT new_order_pk VALIDATE
    EOS
    sqls << sql

    sql = <<-EOS
      ALTER TABLE orders
      MODIFY CONSTRAINT orders_pk VALIDATE
    EOS
    sqls << sql

    sql = <<-EOS
      ALTER TABLE order_line
      MODIFY CONSTRAINT order_line_pk VALIDATE
    EOS
    sqls << sql

    sql = <<-EOS
      ALTER TABLE item
      MODIFY CONSTRAINT item_pk VALIDATE
    EOS
    sqls << sql

    sql = <<-EOS
      ALTER TABLE stock
      MODIFY CONSTRAINT stock_pk VALIDATE
    EOS
    sqls << sql

    # Foreign key
    sql = <<-EOS
      ALTER TABLE district
      MODIFY CONSTRAINT district_fk VALIDATE
    EOS
    sqls << sql

    sql = <<-EOS
      ALTER TABLE customer
      MODIFY CONSTRAINT customer_fk VALIDATE
    EOS
    sqls << sql

    sql = <<-EOS
      ALTER TABLE history
      MODIFY CONSTRAINT history_fk1 VALIDATE
    EOS
    sqls << sql

    sql = <<-EOS
      ALTER TABLE history
      MODIFY CONSTRAINT history_fk2 VALIDATE
    EOS
    sqls << sql

    sql = <<-EOS
      ALTER TABLE new_order
      MODIFY CONSTRAINT new_order_fk VALIDATE
    EOS
    sqls << sql

    sql = <<-EOS
      ALTER TABLE orders
      MODIFY CONSTRAINT orders_fk VALIDATE
    EOS
    sqls << sql

    sql = <<-EOS
      ALTER TABLE order_line
      MODIFY CONSTRAINT order_line_fk1 VALIDATE
    EOS
    sqls << sql

    sql = <<-EOS
      ALTER TABLE order_line
      MODIFY CONSTRAINT order_line_fk2 VALIDATE
    EOS
    sqls << sql

    sql = <<-EOS
      ALTER TABLE stock
      MODIFY CONSTRAINT stock_fk1 VALIDATE
    EOS
    sqls << sql

    sql = <<-EOS
      ALTER TABLE stock
      MODIFY CONSTRAINT stock_fk2 VALIDATE
    EOS
    sqls << sql

    sqls.each { |sql| exec_sql(con, sql.chomp.undent) }
  end

  def analyze(con, parallel_degree)
    sqls = Array.new

    sql = <<-EOS
      DECLARE
        username USER_USERS.USERNAME%TYPE;
      BEGIN
        SELECT USERNAME into username FROM USER_USERS;
        DBMS_STATS.GATHER_TABLE_STATS(ownname => username,
                                      tabname => 'WAREHOUSE',
                                      degree  => #{parallel_degree},
                                      cascade => true);
      END;
    EOS
    sqls << sql

    sql = <<-EOS
      DECLARE
        username USER_USERS.USERNAME%TYPE;
      BEGIN
        SELECT USERNAME into username FROM USER_USERS;
        DBMS_STATS.GATHER_TABLE_STATS(ownname => username,
                                      tabname => 'DISTRICT',
                                      degree  => #{parallel_degree},
                                      cascade => true);
      END;
    EOS
    sqls << sql

    sql = <<-EOS
      DECLARE
        username USER_USERS.USERNAME%TYPE;
      BEGIN
        SELECT USERNAME into username FROM USER_USERS;
        DBMS_STATS.GATHER_TABLE_STATS(ownname => username,
                                      tabname => 'CUSTOMER',
                                      degree  => #{parallel_degree},
                                      cascade => true);
      END;
    EOS
    sqls << sql

    sql = <<-EOS
      DECLARE
        username USER_USERS.USERNAME%TYPE;
      BEGIN
        SELECT USERNAME into username FROM USER_USERS;
        DBMS_STATS.GATHER_TABLE_STATS(ownname => username,
                                      tabname => 'HISTORY',
                                      degree  => #{parallel_degree},
                                      cascade => true);
      END;
    EOS
    sqls << sql

    sql = <<-EOS
      DECLARE
        username USER_USERS.USERNAME%TYPE;
      BEGIN
        SELECT USERNAME into username FROM USER_USERS;
        DBMS_STATS.GATHER_TABLE_STATS(ownname => username,
                                      tabname => 'NEW_ORDER',
                                      degree  => #{parallel_degree},
                                      cascade => true);
      END;
    EOS
    sqls << sql

    sql = <<-EOS
      DECLARE
        username USER_USERS.USERNAME%TYPE;
      BEGIN
        SELECT USERNAME into username FROM USER_USERS;
        DBMS_STATS.GATHER_TABLE_STATS(ownname => username,
                                      tabname => 'ORDERS',
                                      degree  => #{parallel_degree},
                                      cascade => true);
      END;
    EOS
    sqls << sql

    sql = <<-EOS
      DECLARE
        username USER_USERS.USERNAME%TYPE;
      BEGIN
        SELECT USERNAME into username FROM USER_USERS;
        DBMS_STATS.GATHER_TABLE_STATS(ownname => username,
                                      tabname => 'ORDER_LINE',
                                      degree  => #{parallel_degree},
                                      cascade => true);
      END;
    EOS
    sqls << sql

    sql = <<-EOS
      DECLARE
        username USER_USERS.USERNAME%TYPE;
      BEGIN
        SELECT USERNAME into username FROM USER_USERS;
        DBMS_STATS.GATHER_TABLE_STATS(ownname => username,
                                      tabname => 'ITEM',
                                      degree  => #{parallel_degree},
                                      cascade => true);
      END;
    EOS
    sqls << sql

    sql = <<-EOS
      DECLARE
        username USER_USERS.USERNAME%TYPE;
      BEGIN
        SELECT USERNAME into username FROM USER_USERS;
        DBMS_STATS.GATHER_TABLE_STATS(ownname => username,
                                      tabname => 'STOCK',
                                      degree  => #{parallel_degree},
                                      cascade => true);
      END;
    EOS
    sqls << sql

    sqls.each { |sql| exec_sql(con, sql.chomp.undent) }
  end

  def table_partitioning_clause(partition_key, num_key_rows, partition_count, tablespace_name)
    return "" if partition_count == 0
    return "" if num_key_rows / partition_count == 0

    ret = "      PARTITION BY RANGE(#{partition_key})\n" +
          "      (\n"
    1.upto(partition_count) do |n|
      partition_name = sprintf("p%0#{partition_count.to_s.size}d", n)
      value = num_key_rows * n / partition_count + 1
      ret += "        PARTITION #{partition_name} VALUES LESS THAN (#{value}) TABLESPACE #{tablespace_name}#{n == partition_count ? "" : ","}" + "\n"
    end
    ret += "      )\n"

    return ret
  end

  def index_partitioning_clause(partition_count)
    return partition_count > 0 ? "      LOCAL\n" : ""
  end

  def exec_sql(con, sql)
    self.log.info(sql_log_str(sql))
    stmt = nil
    begin
      stmt = con.createStatement
      stmt.executeUpdate(sql)
    ensure
      stmt.close if stmt
    end
  end

  def sql_log_str(sql)
    return ("SQL> " + sql + "\n/").each_line.map { |x| "     " + x }.join.lstrip
  end
end
