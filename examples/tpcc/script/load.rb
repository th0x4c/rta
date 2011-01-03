require 'yaml'
require 'thread'
require 'java'
import java.sql.Date

require File.dirname(__FILE__) + '/helper'

class TPCCLoad < RTA::Session
  include TPCCHelper

  INSERTS_PER_COMMIT = 100

  @@mutex = Mutex.new
  @@count_ware = 0
  @@timestamp = java.sql.Date.new(Time.now.to_f * 1000)
  @@count_load = 0
  @@permutation = Array.new

  def setup
    # ログ
    # self.log = RTA::Log.new(TPCC_HOME + "/log/load_#{@session_id}.log")
    # self.log.level = RTA::Log::DEBUG

    # config.yml の例
    # --
    # Configuration for load and tpcc script
    #
    # tpcc_user: tpcc
    # tpcc_password: tpcc
    # tpcc_url: jdbc:oracle:thin:@192.168.1.5:1521:XE
    # warehouse_range: 1..10
    config = Hash.new
    File.open(TPCC_HOME + '/config/config.yml', 'r') do |file|
      config = YAML.load(file.read)
    end
    warehouse_range = eval(config["warehouse_range"])
    @first_warehouse_id = warehouse_range.first
    @last_warehouse_id =  warehouse_range.last
    @@mutex.synchronize do
      @@count_ware = @last_warehouse_id - @first_warehouse_id + 1
      if @@count_ware <= 0 || @first_warehouse_id <= 0 || @last_warehouse_id <= 0
        log.error("Invalid Warehouse Count.")
        exit(-1)
      end
    end

    log.info("TPCC Data Load Started...")

    # 接続
    begin
      # java.lang.Class.forName("oracle.jdbc.driver.OracleDriver")
      @con = DriverManager.getConnection(config["tpcc_url"],
               config["tpcc_user"], config["tpcc_password"])
      @con.setAutoCommit(false)
    rescue SQLException => e
      e.cause.printStackTrace
    rescue ClassNotFoundException => e
      e.cause.printStackTrace
    end

    # トランザクション
    @tx_last = RTA::Transaction.new("last") do
      @con.commit
    end

    self.go
  end

  def transaction
    tx = nil
    item_ld = MAXITEMS
    ware_ld = item_ld + @@count_ware
    stock_ld = ware_ld + (@@count_ware * MAXITEMS)
    district_ld = stock_ld + (@@count_ware * DIST_PER_WARE)
    cust_ld = district_ld + (@@count_ware * DIST_PER_WARE * CUST_PER_DIST)
    ord_ld = cust_ld + (@@count_ware * DIST_PER_WARE * ORD_PER_DIST)

    @@mutex.synchronize do
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
    @con.commit
    @con.close
    log.info("...DATA LOADING COMPLETED SUCCESSFULLY.")
  end

  def load_items
    unless @load_items
      @load_items = RTA::Transaction.new("load items") do
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
        stmt = @con.prepareStatement(sql)
        stmt.setInt(1, i_id)
        stmt.setInt(2, i_im_id)
        stmt.setString(3, i_name)
        stmt.setFloat(4, i_price)
        stmt.setString(5, i_data)
        stmt.executeUpdate
        stmt.close

        log.info(i_id.to_s) if i_id % 5000 == 0
      end

      @load_items.after_each do
        @con.commit if self.stat.count % INSERTS_PER_COMMIT == 0

        if @loading == MAXITEMS
          @con.commit
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
        w_id = @loading + @first_warehouse_id - 1
        w_name = make_alpha_string(6, 10)
        w_street_1, w_street_2, w_city, w_state, w_zip = make_address
        w_tax = random_number(0, 20) / 100.0 # W_TAX random within [0.0000 .. 0.2000]
        w_ytd = 300000.00 # W_YTD = 300,000.00

        log.debug("WID: #{w_id}, Name: #{w_name}, Tax: #{w_tax}")

        sql = "INSERT INTO warehouse (w_id, w_name, " +
              "w_street_1, w_street_2, w_city, w_state, w_zip, w_tax, w_ytd) " +
              "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)"
        stmt = @con.prepareStatement(sql)
        stmt.setInt(1, w_id)
        stmt.setString(2, w_name)
        stmt.setString(3, w_street_1)
        stmt.setString(4, w_street_2)
        stmt.setString(5, w_city)
        stmt.setString(6, w_state)
        stmt.setString(7, w_zip)
        stmt.setFloat(8, w_tax)
        stmt.setFloat(9, w_ytd)
        stmt.executeUpdate
        stmt.close
      end

      @load_ware.after_each do
        @con.commit if self.stat.count % INSERTS_PER_COMMIT == 0
        @con.commit if @loading == @@count_ware
      end

      @load_ware.whenever_sqlerror { @con.rollback }
    end
    return @load_ware
  end

  def load_stock
    unless @load_stock
      @load_stock = RTA::Transaction.new("load stock") do
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
        stmt = @con.prepareStatement(sql)
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
        stmt.executeUpdate
        stmt.close

        log.info(s_i_id.to_s) if s_i_id % 5000 == 0
      end

      @load_stock.after_each do
        @con.commit if self.stat.count % INSERTS_PER_COMMIT == 0

        if @loading == @@count_ware * MAXITEMS
          @con.commit
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
        stmt = @con.prepareStatement(sql)
        stmt.setInt(1, d_id)
        stmt.setInt(2, d_w_id)
        stmt.setString(3, d_name)
        stmt.setString(4, d_street_1)
        stmt.setString(5, d_street_2)
        stmt.setString(6, d_city)
        stmt.setString(7, d_state)
        stmt.setString(8, d_zip)
        stmt.setFloat(9, d_tax)
        stmt.setFloat(10, d_ytd)
        stmt.setInt(11, d_next_o_id)
        stmt.executeUpdate
        stmt.close
      end

      @load_district.after_each do
        @con.commit if self.stat.count % INSERTS_PER_COMMIT == 0
        @con.commit if @loading == @@count_ware * DIST_PER_WARE
      end

      @load_district.whenever_sqlerror { @con.rollback }
    end
    return @load_district
  end

  def load_cust
    unless @load_cust
      @load_cust = RTA::Transaction.new("load cust") do
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
        stmt = @con.prepareStatement(sql)
        stmt.setInt(1, c_id)
        stmt.setInt(2, c_d_id)
        stmt.setInt(3, c_w_id)
        stmt.setString(4, c_first)
        stmt.setString(5, c_middle)
        stmt.setString(6, c_last)
        stmt.setString(7, c_street_1)
        stmt.setString(8, c_street_2)
        stmt.setString(9, c_city)
        stmt.setString(10, c_state)
        stmt.setString(11, c_zip)
        stmt.setString(12, c_phone)
        stmt.setDate(13, @@timestamp)
        stmt.setString(14, c_credit)
        stmt.setFloat(15, c_credit_lim)
        stmt.setFloat(16, c_discount)
        stmt.setFloat(17, c_balance)
        stmt.setString(18, c_data)
        stmt.executeUpdate
        stmt.close

        h_amount = 10.0
        h_data = make_alpha_string(12, 24)

        sql = "INSERT INTO history (h_c_id, h_c_d_id, h_c_w_id, " +
              "h_w_id, h_d_id, h_date, h_amount, h_data) " +
              "VALUES (?, ?, ?, " +
              "?, ?, ?, ?, ?)"
        stmt = @con.prepareStatement(sql)
        stmt.setInt(1, c_id)
        stmt.setInt(2, c_d_id)
        stmt.setInt(3, c_w_id)
        stmt.setInt(4, c_w_id)
        stmt.setInt(5, c_d_id)
        stmt.setDate(6, @@timestamp)
        stmt.setFloat(7, h_amount)
        stmt.setString(8, h_data)
        stmt.executeUpdate
        stmt.close

        log.info(c_id.to_s) if c_id % 1000 == 0
      end

      @load_cust.after_each do
        @con.commit if self.stat.count % INSERTS_PER_COMMIT == 0

        if @loading == @@count_ware * DIST_PER_WARE * CUST_PER_DIST
          @con.commit
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
          stmt = @con.prepareStatement(sql)
          stmt.setInt(1, o_id)
          stmt.setInt(2, o_c_id)
          stmt.setInt(3, o_d_id)
          stmt.setInt(4, o_w_id)
          stmt.setDate(5, @@timestamp)
          stmt.setInt(6, o_ol_cnt)
          stmt.executeUpdate
          stmt.close

          sql = "INSERT INTO new_order (no_o_id, no_d_id, no_w_id) " +
                "VALUES (?, ?, ?)"
          stmt = @con.prepareStatement(sql)
          stmt.setInt(1, o_id)
          stmt.setInt(2, o_d_id)
          stmt.setInt(3, o_w_id)
          stmt.executeUpdate
          stmt.close
        else
          sql = "INSERT INTO orders (o_id, o_c_id, o_d_id, o_w_id, " +
                "o_entry_d, o_carrier_id, o_ol_cnt, o_all_local) " +
                "VALUES (?, ?, ?, ?, " +
                "?, ?, ?, 1)"
          stmt = @con.prepareStatement(sql)
          stmt.setInt(1, o_id)
          stmt.setInt(2, o_c_id)
          stmt.setInt(3, o_d_id)
          stmt.setInt(4, o_w_id)
          stmt.setDate(5, @@timestamp)
          stmt.setInt(6, o_carrier_id)
          stmt.setInt(7, o_ol_cnt)
          stmt.executeUpdate
          stmt.close
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
            stmt = @con.prepareStatement(sql)
            stmt.setInt(1, o_id)
            stmt.setInt(2, o_d_id)
            stmt.setInt(3, o_w_id)
            stmt.setInt(4, ol)
            stmt.setInt(5, ol_i_id)
            stmt.setInt(6, ol_supply_w_id)
            stmt.setInt(7, ol_quantity)
            stmt.setFloat(8, ol_amount)
            stmt.setString(9, ol_dist_info)
            stmt.executeUpdate
            stmt.close
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
            stmt = @con.prepareStatement(sql)
            stmt.setInt(1, o_id)
            stmt.setInt(2, o_d_id)
            stmt.setInt(3, o_w_id)
            stmt.setInt(4, ol)
            stmt.setInt(5, ol_i_id)
            stmt.setInt(6, ol_supply_w_id)
            stmt.setInt(7, ol_quantity)
            stmt.setFloat(8, ol_amount)
            stmt.setString(9, ol_dist_info)
            stmt.setDate(10, @@timestamp)
            stmt.executeUpdate
            stmt.close
          end
        end

        log.info(o_id.to_s) if o_id % 1000 == 0
      end

      @load_ord.after_each do
        @con.commit if self.stat.count % INSERTS_PER_COMMIT == 0

        if @loading == @@count_ware * DIST_PER_WARE * ORD_PER_DIST
          @con.commit
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
    @@mutex.synchronize do
      ret = @@permutation.shift
    end
    raise "permutation is nil" if ret.nil?
    return ret
  end
end
