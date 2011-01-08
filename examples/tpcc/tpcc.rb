require 'yaml'
require 'thread'
import java.sql.Timestamp

require File.dirname(__FILE__) + '/script/helper'

class TPCC < RTA::Session
  include TPCCHelper

  UNUSED_I_ID = -1

  INVALID_ITEM_ERROR_CODE = -1
  INVALID_ITEM_SQL_EXCEPTION = SQLException.new.initCause(SQLException.new("Item number is not valid", nil, INVALID_ITEM_ERROR_CODE))

  NOT_FOUND_ERROR_CODE = -2
  NOT_FOUND_SQL_EXCEPTION = SQLException.new.initCause(SQLException.new("Not found", nil, NOT_FOUND_ERROR_CODE))

  @@time_str = Time.now.strftime("%Y%m%d%H%M%S")

  def setup
    # ログ
    self.log = RTA::Log.new(TPCC_HOME + "/log/tpcc_#{@@time_str}.log")
    # self.log.level = RTA::Log::DEBUG

    # config.yml の例
    # ---
    # Configuration for load and tpcc script
    #
    # tpcc_user: tpcc
    # tpcc_password: tpcc
    # tpcc_url: jdbc:oracle:thin:@192.168.1.5:1521:XE
    # warehouse_range: 1..10
    # think_time: # Think time
    #   new_order:    0.01  # New-Order
    #   payment:      0.01  # Payment
    #   order_status: 0.01  # Order-Status
    #   delivery:     0.01  # Delivery
    #   stock_level:  0.01  # Stock-Level
    #
    # tx_percentage: # Percentage of each transaction
    #   new_order:    45.0  # New-Order
    #   payment:      43.0  # Payment
    #   order_status:  4.0  # Order-Status
    #   delivery:      4.0  # Delivery
    #   stock_level:   4.0  # Stock-Level
    config = Hash.new
    File.open(TPCC_HOME + '/config/config.yml', 'r') do |file|
      config = YAML.load(file.read)
    end
    warehouse_range = eval(config["warehouse_range"])
    @first_w_id = warehouse_range.first
    @last_w_id =  warehouse_range.last
    if @first_w_id <= 0 || @last_w_id <= 0
      log.error("Invalid Warehouse Count.")
      exit(-1)
    end
    @think_time = config["think_time"]
    @tx_percentage = config["tx_percentage"]
    # @tx_percentage を % に変換(合計が 100 となるようにする)
    sum = @tx_percentage.values.inject(0) { |sum, pct| sum + pct }
    @tx_percentage.each { |key, value| @tx_percentage[key] = (value / sum.to_f) * 100 }

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

    # Transaction
    @tx = Hash.new
    @tx["new_order"] = neword   # New-Order Transaction  
    @tx["payment"] = payment    # Payment Transaction    
    @tx["order_status"] = ostat # Order-Status Transaction
    @tx["delivery"] = delivery  # Delivery Transaction   
    @tx["stock_level"] = slev   # Stock-Level Transaction
  end

  def transaction
    rand_pct = rand * 100 # rand returns a pseudorandom floating point number
                          # greater than or equal to 0.0 and less than 1.0
    cul = 0
    @tx.each do |tx_name, tx|
      cul += @tx_percentage[tx_name]
      return tx if rand_pct < cul
    end

    # Unreachable unless all transaction percentages are 0
    log.fatal("No transaction available")
    raise "No transaction available"
  end

  def teardown
    @con.close
  end

  def transactions
    return @tx.values
  end

  # New-Order Transaction
  def neword
    tx = RTA::Transaction.new("New-Order") do
      datetime = java.sql.Timestamp.new(Time.now.to_f * 1000)

      sql1 = "SELECT c_discount, c_last, c_credit, w_tax " +
             "FROM customer, warehouse " +
             "WHERE w_id = ? AND c_w_id = w_id AND " +
             "  c_d_id = ? AND c_id = ?"
      stmt1 = @con.prepareStatement(sql1)
      stmt1.setInt(1, @input[:w_id])
      stmt1.setInt(2, @input[:d_id])
      stmt1.setInt(3, @input[:c_id])
      rset = stmt1.executeQuery
      rownum = 0
      while rset.next
        c_discount = rset.getFloat("c_discount")
        c_last = rset.getString("c_last")
        c_credit = rset.getString("c_credit")
        w_tax = rset.getFloat("w_tax")
        rownum += 1
      end
      rset.close
      stmt1.close
      raise NOT_FOUND_SQL_EXCEPTION if rownum == 0

      sql2 = "SELECT d_next_o_id, d_tax " +
             "FROM district "+
             "WHERE d_id = ? AND d_w_id = ?"
      stmt2 = @con.prepareStatement(sql2)
      stmt2.setInt(1, @input[:d_id])
      stmt2.setInt(2, @input[:w_id])
      rset = stmt2.executeQuery
      rownum = 0
      while rset.next
        d_next_o_id = rset.getInt("d_next_o_id")
        d_tax = rset.getFloat("d_tax")
        rownum += 1
      end
      rset.close
      stmt2.close
      raise NOT_FOUND_SQL_EXCEPTION if rownum == 0

      sql3 = "UPDATE district SET d_next_o_id = ? + 1 " +
             "WHERE d_id = ? AND d_w_id = ?"
      stmt3 = @con.prepareStatement(sql3)
      stmt3.setInt(1, d_next_o_id)
      stmt3.setInt(2, @input[:d_id])
      stmt3.setInt(3, @input[:w_id])
      stmt3.executeUpdate
      stmt3.close

      o_id = d_next_o_id

      sql4 = "INSERT INTO orders (o_id, o_d_id, o_w_id, o_c_id, " +
             "  o_entry_d, o_carrier_id, o_ol_cnt, o_all_local) " +
             "VALUES (?, ?, ?, ?, ?, NULL, ?, ?)"
      stmt4 = @con.prepareStatement(sql4)
      stmt4.setInt(1, o_id)
      stmt4.setInt(2, @input[:d_id])
      stmt4.setInt(3, @input[:w_id])
      stmt4.setInt(4, @input[:c_id])
      stmt4.setTimestamp(5, datetime)
      stmt4.setInt(6, @input[:o_ol_cnt])
      stmt4.setInt(7, @input[:o_all_local])
      stmt4.executeUpdate
      stmt4.close

      sql5 = "INSERT INTO new_order (no_o_id, no_d_id, no_w_id) " +
             "VALUES (?, ?, ?)"
      stmt5 = @con.prepareStatement(sql5)
      stmt5.setInt(1, o_id)
      stmt5.setInt(2, @input[:d_id])
      stmt5.setInt(3, @input[:w_id])
      stmt5.executeUpdate
      stmt5.close

      price = Array.new
      iname = Array.new
      stock = Array.new
      bg = Array.new
      amt = Array.new
      total = 0
      @input[:o_ol_cnt].times do |ol_number|
        ol_supply_w_id = @input[:supware][ol_number]
        ol_i_id = @input[:itemid][ol_number]
        ol_quantity = @input[:qty][ol_number]

        sql6 = "SELECT i_price, i_name , i_data " +
               "FROM item " +
               "WHERE i_id = ?"
        stmt6 = @con.prepareStatement(sql6)
        stmt6.setInt(1, ol_i_id)
        rset = stmt6.executeQuery
        rownum = 0
        while rset.next
          price[ol_number] = rset.getFloat("i_price")
          iname[ol_number] = rset.getString("i_name")
          i_data = rset.getString("i_data")
          rownum += 1
        end
        rset.close
        stmt6.close
        raise INVALID_ITEM_SQL_EXCEPTION if rownum == 0

        sql7 =  "SELECT s_quantity, s_data, " +
                "  s_dist_01, s_dist_02, s_dist_03, s_dist_04, s_dist_05, " +
                "  s_dist_06, s_dist_07, s_dist_08, s_dist_09, s_dist_10 " +
                "FROM stock " +
                "WHERE s_i_id = ? AND s_w_id = ?"
        stmt7 = @con.prepareStatement(sql7)
        stmt7.setInt(1, ol_i_id)
        stmt7.setInt(2, ol_supply_w_id)
        rset = stmt7.executeQuery
        s_dist_xx = Array.new
        rownum = 0
        while rset.next
          s_quantity = rset.getInt("s_quantity")
          s_data = rset.getString("s_data")
          s_dist_xx << rset.getString("s_dist_01")
          s_dist_xx << rset.getString("s_dist_02")
          s_dist_xx << rset.getString("s_dist_03")
          s_dist_xx << rset.getString("s_dist_04")
          s_dist_xx << rset.getString("s_dist_05")
          s_dist_xx << rset.getString("s_dist_06")
          s_dist_xx << rset.getString("s_dist_07")
          s_dist_xx << rset.getString("s_dist_08")
          s_dist_xx << rset.getString("s_dist_09")
          s_dist_xx << rset.getString("s_dist_10")
          rownum += 1
        end
        rset.close
        stmt7.close
        raise NOT_FOUND_SQL_EXCEPTION if rownum == 0

        ol_dist_info = s_dist_xx[@input[:d_id] - 1] # pick correct s_dist_xx
        stock[ol_number] = s_quantity

        if i_data =~ /original/ && s_data =~ /original/
          bg[ol_number] = 'B'
        else
          bg[ol_number] = 'G'
        end

        # If the retrieved value for S_QUANTITY exceeds OL_QUANTITY by 10 or
        # more, then S_QUANTITY is decreased by OL_QUANTITY; otherwise
        # S_QUANTITY is updated to (S_QUANTITY - OL_QUANTITY)+91.
        if s_quantity >= ol_quantity + 10
          s_quantity = s_quantity - ol_quantity
        else
          s_quantity = s_quantity - ol_quantity + 91
        end

        # S_YTD is increased by OL_QUANTITY and S_ORDER_CNT is incremented by 1.
        # If the order-line is remote, then S_REMOTE_CNT is incremented by 1.
        remote = (@input[:w_id] != ol_supply_w_id) ? 1 : 0
        sql8 = "UPDATE stock SET s_quantity = ?, " +
               "  s_ytd = s_ytd + ?, s_order_cnt = s_order_cnt + 1, " +
               "  s_remote_cnt = s_remote_cnt + ? " +
               "WHERE s_i_id = ? " +
               "AND s_w_id = ?"
        stmt8 = @con.prepareStatement(sql8)
        stmt8.setInt(1, s_quantity)
        stmt8.setInt(2, ol_quantity)
        stmt8.setInt(3, remote)
        stmt8.setInt(4, ol_i_id)
        stmt8.setInt(5, ol_supply_w_id)
        stmt8.executeUpdate
        stmt8.close

        # The amount for the item in the order (OL_AMOUNT) is computed as:
        #   OL_QUANTITY*I_PRICE
        ol_amount = ol_quantity * price[ol_number]
        amt[ol_number] = ol_amount
        # The total-amount for the complete order is computed as:
        #   sum(OL_AMOUNT) *(1 - C_DISCOUNT) *(1 + W_TAX + D_TAX)
        total += ol_amount * (1 + w_tax + d_tax) * (1 - c_discount)

        sql9 = "INSERT INTO order_line (ol_o_id, ol_d_id, ol_w_id, ol_number, " +
               "  ol_i_id, ol_supply_w_id, " +
               "  ol_quantity, ol_amount, ol_dist_info) " +
               "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)"
        stmt9 = @con.prepareStatement(sql9)
        stmt9.setInt(1, o_id)
        stmt9.setInt(2, @input[:d_id])
        stmt9.setInt(3, @input[:w_id])
        stmt9.setInt(4, ol_number + 1)
        stmt9.setInt(5, ol_i_id)
        stmt9.setInt(6, ol_supply_w_id)
        stmt9.setInt(7, ol_quantity)
        stmt9.setFloat(8, ol_amount)
        stmt9.setString(9, ol_dist_info)
        stmt9.executeUpdate
        stmt9.close
      end

      @con.commit
    end

    tx.before_each do
      w_id = home_w_id
      d_id = random_number(1, DIST_PER_WARE)
      c_id = nurand(1023, 1, CUST_PER_DIST)
      ol_cnt = random_number(5, 15)

      rbk = random_number(1, 100)

      supware = Array.new
      itemid = Array.new
      qty = Array.new
      home = true
      ol_cnt.times do |idx|
        if idx == ol_cnt -1 && rbk == 1
          ol_i_id = UNUSED_I_ID
        else
          ol_i_id = nurand(8191, 1, MAXITEMS)
        end

        ol_supply_w_id = w_id
        if count_ware > 1
          if random_number(1, 100) == 1
            ary = Array(@first_w_id .. @last_w_id) - [w_id]
            ol_supply_w_id = ary[rand(ary.size)]
            home = false
          end
        end

        ol_quantity = random_number(1, 10)

        supware << ol_supply_w_id
        itemid << ol_i_id
        qty << ol_quantity 
      end

      @input = Hash.new
      @input[:w_id] = w_id
      @input[:d_id] = d_id
      @input[:c_id] = c_id
      @input[:o_ol_cnt] = ol_cnt
      @input[:o_all_local] = home ? 1 : 0
      @input[:supware] = supware
      @input[:itemid] = itemid
      @input[:qty] = qty
    end

    tx.after_each { sleep(@think_time["new_order"] || 0) }

    tx.whenever_sqlerror do |ex|
      @con.rollback
      log.warn(ex.getMessage.chomp) if ex.getErrorCode == INVALID_ITEM_ERROR_CODE
      log.debug(YAML.dump(@input).chomp)
    end

    return tx
  end

  # Payment Transaction
  def payment
    tx = RTA::Transaction.new("Payment") do
      datetime = java.sql.Timestamp.new(Time.now.to_f * 1000)

      sql1 = "UPDATE warehouse SET w_ytd = w_ytd + ? " +
             "WHERE w_id = ?"
      stmt1 = @con.prepareStatement(sql1)
      stmt1.setFloat(1, @input[:h_amount])
      stmt1.setInt(2, @input[:w_id])
      stmt1.executeUpdate
      stmt1.close

      sql2 = "SELECT w_street_1, w_street_2, w_city, w_state, w_zip, w_name " +
             "FROM warehouse " +
             "WHERE w_id = ?"
      stmt2 = @con.prepareStatement(sql2)
      stmt2.setInt(1, @input[:w_id])
      rset = stmt2.executeQuery
      rownum = 0
      while rset.next
        w_street_1 = rset.getString("w_street_1")
        w_street_2 = rset.getString("w_street_2")
        w_city = rset.getString("w_city")
        w_state = rset.getString("w_state")
        w_zip = rset.getString("w_zip")
        w_name = rset.getString("w_name")
        rownum += 1
      end
      rset.close
      stmt2.close
      raise NOT_FOUND_SQL_EXCEPTION if rownum == 0

      sql3 = "UPDATE district SET d_ytd = d_ytd + ? " +
             "WHERE d_w_id = ? AND d_id = ?"
      stmt3 = @con.prepareStatement(sql3)
      stmt3.setFloat(1, @input[:h_amount])
      stmt3.setInt(2, @input[:w_id])
      stmt3.setInt(3, @input[:d_id])
      stmt3.executeUpdate
      stmt3.close

      sql4 = "SELECT d_street_1, d_street_2, d_city, d_state, d_zip, d_name " +
             "FROM district " +
             "WHERE d_w_id = ? AND d_id = ?"
      stmt4 = @con.prepareStatement(sql4)
      stmt4.setInt(1, @input[:w_id])
      stmt4.setInt(2, @input[:d_id])
      rset = stmt4.executeQuery
      rownum = 0
      while rset.next
        d_street_1 = rset.getString("d_street_1")
        d_street_2 = rset.getString("d_street_2")
        d_city = rset.getString("d_city")
        d_state = rset.getString("d_state")
        d_zip = rset.getString("d_zip")
        d_name = rset.getString("d_name")
        rownum += 1
      end
      rset.close
      stmt4.close
      raise NOT_FOUND_SQL_EXCEPTION if rownum == 0

      c_id = @input[:c_id]
      if @input[:byname]
        sql5 = "SELECT count(c_id) " +
               "FROM customer " +
               "WHERE c_last = ? AND c_d_id = ? AND c_w_id = ?"
        stmt5 = @con.prepareStatement(sql5)
        stmt5.setString(1, @input[:c_last])
        stmt5.setInt(2, @input[:c_d_id])
        stmt5.setInt(3, @input[:c_w_id])
        rset = stmt5.executeQuery
        while rset.next
          namecnt = rset.getInt(1)
        end
        rset.close
        stmt5.close
        raise NOT_FOUND_SQL_EXCEPTION if namecnt == 0

        sql6 = "SELECT c_first, c_middle, c_id, " +
               "  c_street_1, c_street_2, c_city, c_state, c_zip, " +
               "  c_phone, c_credit, c_credit_lim, " +
               "  c_discount, c_balance, c_since " +
               "FROM customer " +
               "WHERE c_w_id = ? AND c_d_id = ? AND c_last = ? " +
               "ORDER BY c_first"
        stmt6 = @con.prepareStatement(sql6)
        stmt6.setInt(1, @input[:c_w_id])
        stmt6.setInt(2, @input[:c_d_id])
        stmt6.setString(3, @input[:c_last])
        c_byname = stmt6.executeQuery

        namecnt += 1 if namecnt % 2 == 1
        (namecnt / 2).times do |n|
          c_byname.next
          c_first = c_byname.getString("c_first")
          c_middle = c_byname.getString("c_middle")
          c_id = c_byname.getInt("c_id")
          c_street_1 = c_byname.getString("c_street_1")
          c_street_2 = c_byname.getString("c_street_2")
          c_city = c_byname.getString("c_city")
          c_state = c_byname.getString("c_state")
          c_zip = c_byname.getString("c_zip")
          c_phone = c_byname.getString("c_phone")
          c_credit = c_byname.getString("c_credit")
          c_credit_lim = c_byname.getFloat("c_credit_lim")
          c_discount = c_byname.getFloat("c_discount")
          c_balance = c_byname.getFloat("c_balance")
          c_since = c_byname.getTimestamp("c_since")
        end
        c_byname.close
        stmt6.close
      else
        sql7 = "SELECT c_first, c_middle, c_last, " +
               "  c_street_1, c_street_2, c_city, c_state, c_zip, " +
               "  c_phone, c_credit, c_credit_lim, " +
               "  c_discount, c_balance, c_since " +
               "FROM customer " +
               "WHERE c_w_id = ? AND c_d_id = ? AND c_id = ?"
        stmt7 = @con.prepareStatement(sql7)
        stmt7.setInt(1, @input[:c_w_id])
        stmt7.setInt(2, @input[:c_d_id])
        stmt7.setInt(3, @input[:c_id])
        rset = stmt7.executeQuery
        rownum = 0
        while rset.next
          c_first = rset.getString("c_first")
          c_middle = rset.getString("c_middle")
          c_last = rset.getString("c_last")
          c_street_1 = rset.getString("c_street_1")
          c_street_2 = rset.getString("c_street_2")
          c_city = rset.getString("c_city")
          c_state = rset.getString("c_state")
          c_zip = rset.getString("c_zip")
          c_phone = rset.getString("c_phone")
          c_credit = rset.getString("c_credit")
          c_credit_lim = rset.getFloat("c_credit_lim")
          c_discount = rset.getFloat("c_discount")
          c_balance = rset.getFloat("c_balance")
          c_since = rset.getTimestamp("c_since")
          rownum += 1
        end
        rset.close
        stmt7.close
        raise NOT_FOUND_SQL_EXCEPTION if rownum == 0
      end

      if c_credit == "BC"
        sql8 = "SELECT c_data " +
               "FROM customer " +
               "WHERE c_w_id = ? AND c_d_id = ? AND c_id = ?"
        stmt8 = @con.prepareStatement(sql8)
        stmt8.setInt(1, @input[:c_w_id])
        stmt8.setInt(2, @input[:c_d_id])
        stmt8.setInt(3, c_id)
        rset = stmt8.executeQuery
        rownum = 0
        while rset.next
          c_data = rset.getString("c_data")
          rownum += 1
        end
        rset.close
        stmt8.close
        raise NOT_FOUND_SQL_EXCEPTION if rownum == 0

        # If the value of C_CREDIT is equal to "BC", then C_DATA is also
        # retrieved from the selected customer and the following history
        # information: C_ID, C_D_ID, C_W_ID, D_ID, W_ID, and H_AMOUNT, are
        # inserted at the left of the C_DATA field by shifting the existing
        # content of C_DATA to the right by an equal number of bytes and by
        # discarding the bytes that are shifted out of the right side of the
        # C_DATA field. The content of the C_DATA field never exceeds 500
        # characters. The selected customer is updated with the new C_DATA field.
        # If C_DATA is implemented as two fields (see Clause 1.4.9), they must
        # be treated and operated on as one single field.
        c_new_data = ["|", c_id, @input[:c_d_id], @input[:c_w_id],
                      @input[:d_id], @input[:w_id], @input[:h_amount]].join(" ")
        c_new_data = (c_new_data + c_data)[0, 500]

        # C_BALANCE is decreased by H_AMOUNT. C_YTD_PAYMENT is increased by
        # H_AMOUNT. C_PAYMENT_CNT is incremented by 1.
        sql9 = "UPDATE customer " +
               "SET c_balance = c_balance - ?, c_data = ?, " +
               "  c_ytd_payment = c_ytd_payment + ?, c_payment_cnt = c_payment_cnt + 1 " +
               "WHERE c_w_id = ? AND c_d_id = ? AND " +
               "  c_id = ?"
        stmt9 = @con.prepareStatement(sql9)
        stmt9.setFloat(1, @input[:h_amount])
        stmt9.setString(2, c_new_data)
        stmt9.setFloat(3, @input[:h_amount])
        stmt9.setInt(4, @input[:c_w_id])
        stmt9.setInt(5, @input[:c_d_id])
        stmt9.setInt(6, c_id)
        stmt9.executeUpdate
        stmt9.close
      else
        # C_BALANCE is decreased by H_AMOUNT. C_YTD_PAYMENT is increased by
        # H_AMOUNT. C_PAYMENT_CNT is incremented by 1.
        sql10 = "UPDATE customer " +
                "SET c_balance = c_balance - ?, " +
               "  c_ytd_payment = c_ytd_payment + ?, c_payment_cnt = c_payment_cnt + 1 " +
                "WHERE c_w_id = ? AND c_d_id = ? AND " +
                "  c_id = ?"
        stmt10 = @con.prepareStatement(sql10)
        stmt10.setFloat(1, @input[:h_amount])
        stmt10.setFloat(2, @input[:h_amount])
        stmt10.setInt(3, @input[:c_w_id])
        stmt10.setInt(4, @input[:c_d_id])
        stmt10.setInt(5, c_id)
        stmt10.executeUpdate
        stmt10.close
      end

      # H_DATA is built by concatenating W_NAME and D_NAME separated by 4 spaces.
      h_data = w_name + "    " + d_name

      sql11 = "INSERT INTO history (h_c_d_id, h_c_w_id, h_c_id, h_d_id, " +
              "  h_w_id, h_date, h_amount, h_data) " +
              "VALUES (?, ?, ?, ?, " +
              "  ?, ?, ?, ?)"
      stmt11 = @con.prepareStatement(sql11)
      stmt11.setInt(1, @input[:c_d_id])
      stmt11.setInt(2, @input[:c_w_id])
      stmt11.setInt(3, c_id)
      stmt11.setInt(4, @input[:d_id])
      stmt11.setInt(5, @input[:w_id])
      stmt11.setTimestamp(6, datetime)
      stmt11.setFloat(7, @input[:h_amount])
      stmt11.setString(8, h_data)
      stmt11.executeUpdate
      stmt11.close

      @con.commit
    end

    tx.before_each do
      w_id = home_w_id
      d_id = random_number(1, DIST_PER_WARE)

      x = random_number(1, 100)
      y = random_number(1, 100)
      if x <= 85
        c_d_id = d_id
        c_w_id = w_id
        home = true
      else
        c_d_id = random_number(1, DIST_PER_WARE)
        if count_ware > 1
          ary = Array(@first_w_id .. @last_w_id) - [w_id]
          c_w_id = ary[rand(ary.size)]
          home = false
        else
          c_w_id = w_id
          home = true
        end
      end
      if y <= 60
        c_last = lastname(nurand(255, 0, 999))
        byname = true
      else
        c_id = nurand(1023, 1, CUST_PER_DIST)
        byname = false
      end

      h_amount = random_number(100, 500000) / 100.0

      @input = Hash.new
      @input[:w_id] = w_id
      @input[:d_id] = d_id
      @input[:c_d_id] = c_d_id
      @input[:c_w_id] = c_w_id
      @input[:c_last] = c_last
      @input[:c_id] = c_id
      @input[:byname] = byname
      @input[:h_amount] = h_amount
    end

    tx.after_each { sleep(@think_time["payment"] || 0) }

    tx.whenever_sqlerror do
      @con.rollback
      log.debug(YAML.dump(@input).chomp)
    end

    return tx
  end

  # Order-Status Transaction
  def ostat
    tx = RTA::Transaction.new("Order-Status") do
      c_id = @input[:c_id]
      if @input[:byname]
        sql1 = "SELECT count(c_id) " +
               "FROM customer " +
               "WHERE c_last = ? AND c_d_id = ? AND c_w_id = ?"
        stmt1 = @con.prepareStatement(sql1)
        stmt1.setString(1, @input[:c_last])
        stmt1.setInt(2, @input[:d_id])
        stmt1.setInt(3, @input[:w_id])
        rset = stmt1.executeQuery
        while rset.next
          namecnt = rset.getInt(1)
        end
        rset.close
        stmt1.close
        raise NOT_FOUND_SQL_EXCEPTION if namecnt == 0

        sql2 = "SELECT c_balance, c_first, c_middle, c_id " +
               "FROM customer " +
               "WHERE c_last = ? AND c_d_id = ? AND c_w_id = ?" +
               "ORDER BY c_first"
        stmt2 = @con.prepareStatement(sql2)
        stmt2.setString(1, @input[:c_last])
        stmt2.setInt(2, @input[:d_id])
        stmt2.setInt(3, @input[:w_id])
        c_name = stmt2.executeQuery

        namecnt += 1 if namecnt % 2 == 1
        (namecnt / 2).times do |n|
          c_name.next
          c_balance = c_name.getFloat("c_balance")
          c_first = c_name.getString("c_first")
          c_middle = c_name.getString("c_middle")
          c_id = c_name.getInt("c_id")
        end
        c_name.close
        stmt2.close
      else
        sql3 = "SELECT c_balance, c_first, c_middle, c_last " +
               "FROM customer " +
               "WHERE c_id = ? AND c_d_id = ? AND c_w_id = ?"
        stmt3 = @con.prepareStatement(sql3)
        stmt3.setInt(1, @input[:c_id])
        stmt3.setInt(2, @input[:d_id])
        stmt3.setInt(3, @input[:w_id])
        rset = stmt3.executeQuery
        rownum = 0
        while rset.next
          c_balance = rset.getFloat("c_balance")
          c_first = rset.getString("c_first")
          c_middle = rset.getString("c_middle")
          c_last = rset.getString("c_last")
          rownum += 1
        end
        rset.close
        stmt3.close
        raise NOT_FOUND_SQL_EXCEPTION if rownum == 0
      end

      # The row in the ORDER table with matching O_W_ID (equals C_W_ID), O_D_ID
      # (equals C_D_ID), O_C_ID (equals C_ID), and with the largest existing
      # O_ID, is selected. This is the most recent order placed by that customer.
      # O_ID, O_ENTRY_D, and O_CARRIER_ID are retrieved.
      sql4 = "SELECT o_id, o_carrier_id, o_entry_d " +
             "FROM orders " +
             "WHERE o_w_id = ? AND o_d_id = ? AND o_c_id = ? AND " +
             "  o_id = (SELECT MAX(o_id) FROM orders " +
             "          WHERE o_w_id = ? AND o_d_id = ? AND o_c_id = ?)"
      stmt4 = @con.prepareStatement(sql4)
      stmt4.setInt(1, @input[:w_id])
      stmt4.setInt(2, @input[:d_id])
      stmt4.setInt(3, c_id)
      stmt4.setInt(4, @input[:w_id])
      stmt4.setInt(5, @input[:d_id])
      stmt4.setInt(6, c_id)
      rset = stmt4.executeQuery
      rownum = 0
      while rset.next
        o_id = rset.getInt("o_id")
        o_carrier_id = rset.getInt("o_carrier_id")
        entdate = rset.getTimestamp("o_entry_d")
        rownum += 1
      end
      rset.close
      stmt4.close
      raise NOT_FOUND_SQL_EXCEPTION if rownum == 0

      sql5 = "SELECT ol_i_id, ol_supply_w_id, ol_quantity, " +
             "  ol_amount, ol_delivery_d " +
             "FROM order_line " +
             "WHERE ol_o_id = ? AND ol_d_id = ? AND ol_w_id = ?"
      stmt5 = @con.prepareStatement(sql5)
      stmt5.setInt(1, o_id)
      stmt5.setInt(2, @input[:d_id])
      stmt5.setInt(3, @input[:w_id])
      c_line = stmt5.executeQuery
      ol_i_id = Array.new
      ol_supply_w_id = Array.new
      ol_quantity = Array.new
      ol_amount = Array.new
      ol_delivery_d = Array.new
      while c_line.next
        ol_i_id << c_line.getInt("ol_i_id")
        ol_supply_w_id << c_line.getInt("ol_supply_w_id")
        ol_quantity << c_line.getInt("ol_quantity") 
        ol_amount << c_line.getFloat("ol_amount") 
        ol_delivery_d << c_line.getTimestamp("ol_delivery_d")
      end
      c_line.close
      stmt5.close

      # Comment: a commit is not required as long as all ACID properties are
      # satisfied (see Clause 3).
      # @con.commit
    end

    tx.before_each do
      w_id = home_w_id
      d_id = random_number(1, DIST_PER_WARE)

      y = random_number(1, 100)
      if y <= 60
        c_last = lastname(nurand(255, 0, 999))
        byname = true
      else
        c_id = nurand(1023, 1, CUST_PER_DIST)
        byname = false
      end

      @input = Hash.new
      @input[:w_id] = w_id
      @input[:d_id] = d_id
      @input[:c_last] = c_last
      @input[:c_id] = c_id
      @input[:byname] = byname
    end

    tx.after_each { sleep(@think_time["order_status"] || 0) }

    tx.whenever_sqlerror do
      @con.rollback
      log.debug(YAML.dump(@input).chomp)
    end

    return tx
  end

  # Delivery Transaction
  def delivery
    tx = RTA::Transaction.new("Delivery") do
      datetime = java.sql.Timestamp.new(Time.now.to_f * 1000)

      # Upon completion of the business transaction, the following information
      # must have been recorded into a result file:
      #   o The time at which the business transaction was queued.
      #   o The warehouse number (W_ID) and the carried number (O_CARRIER_ID)
      #     associated with the business transaction.
      #   o The district number (D_ID) and the order number (O_ID) of each order
      #     delivered by the business transaction.
      #   o The time at which the business transaction completed.
      log.info("W: #{@input[:w_id]}, C: #{@input[:o_carrier_id]}")

      1.upto(DIST_PER_WARE) do |d_id|
        # The row in the NEW-ORDER table with matching NO_W_ID (equals W_ID) and
        # NO_D_ID (equals D_ID) and with the lowest NO_O_ID value is selected.
        # This is the oldest undelivered order of that district. NO_O_ID, the
        # order number, is retrieved. If no matching row is found, then the
        # delivery of an order for this district is skipped.
        sql1 = "SELECT MIN(no_o_id) " +
               "FROM new_order " +
               "WHERE no_d_id = ? AND no_w_id = ?"
        stmt1 = @con.prepareStatement(sql1)
        stmt1.setInt(1, d_id)
        stmt1.setInt(2, @input[:w_id])
        c_no = stmt1.executeQuery
        rownum = 0
        nullrow = true
        while c_no.next
          no_o_id = c_no.getInt(1)
          rownum += 1
          nullrow = false unless c_no.wasNull
        end
        c_no.close
        stmt1.close
        next if rownum == 0 || nullrow

        sql2 = "DELETE FROM new_order " +
               "WHERE no_w_id = ? AND no_d_id = ? AND no_o_id = ?"
        stmt2 = @con.prepareStatement(sql2)
        stmt2.setInt(1, @input[:w_id])
        stmt2.setInt(2, d_id)
        stmt2.setInt(3, no_o_id)
        stmt2.executeUpdate
        stmt2.close

        sql3 = "SELECT o_c_id " +
               "FROM orders " +
               "WHERE o_id = ? AND o_d_id = ? AND " +
               "  o_w_id = ?"
        stmt3 = @con.prepareStatement(sql3)
        stmt3.setInt(1, no_o_id)
        stmt3.setInt(2, d_id)
        stmt3.setInt(3, @input[:w_id])
        rset = stmt3.executeQuery
        rownum = 0
        while rset.next
          c_id = rset.getInt("o_c_id")
          rownum += 1
        end
        rset.close
        stmt3.close
        raise NOT_FOUND_SQL_EXCEPTION if rownum == 0

        sql4 = "UPDATE orders SET o_carrier_id = ? " +
               "WHERE o_id = ? AND o_d_id = ? AND " +
               "  o_w_id = ?"
        stmt4 = @con.prepareStatement(sql4)
        stmt4.setInt(1, @input[:o_carrier_id])
        stmt4.setInt(2, no_o_id)
        stmt4.setInt(3, d_id)
        stmt4.setInt(4, @input[:w_id])
        stmt4.executeUpdate
        stmt4.close

        sql5 = "UPDATE order_line SET ol_delivery_d = ? " +
               "WHERE ol_o_id = ? AND ol_d_id = ? AND " +
               "  ol_w_id = ?"
        stmt5 = @con.prepareStatement(sql5)
        stmt5.setTimestamp(1, datetime)
        stmt5.setInt(2, no_o_id)
        stmt5.setInt(3, d_id)
        stmt5.setInt(4, @input[:w_id])
        stmt5.executeUpdate
        stmt5.close

        sql6 = "SELECT SUM(ol_amount) " +
               "FROM order_line " +
               "WHERE ol_o_id = ? AND ol_d_id = ? AND " +
               "  ol_w_id = ?"
        stmt6 = @con.prepareStatement(sql6)
        stmt6.setInt(1, no_o_id)
        stmt6.setInt(2, d_id)
        stmt6.setInt(3, @input[:w_id])
        rset = stmt6.executeQuery
        rownum = 0
        nullrow = true
        while rset.next
          ol_total = rset.getFloat(1)
          rownum += 1
          nullrow = false unless rset.wasNull
        end
        rset.close
        stmt6.close
        raise NOT_FOUND_SQL_EXCEPTION if rownum == 0 || nullrow

        # The row in the CUSTOMER table with matching C_W_ID (equals W_ID),
        # C_D_ID (equals D_ID), and C_ID (equals O_C_ID) is selected and
        # C_BALANCE is increased by the sum of all order-line amounts (OL_AMOUNT)
        # previously retrieved. C_DELIVERY_CNT is incremented by 1.
        sql7 = "UPDATE customer SET c_balance = c_balance + ?, " +
               "  c_delivery_cnt = c_delivery_cnt + 1 " +
               "WHERE c_id = ? AND c_d_id = ? AND " +
               "  c_w_id = ?"
        stmt7 = @con.prepareStatement(sql7)
        stmt7.setFloat(1, ol_total)
        stmt7.setInt(2, c_id)
        stmt7.setInt(3, d_id)
        stmt7.setInt(4, @input[:w_id])
        stmt7.executeUpdate
        stmt7.close

        @con.commit
        log.info("D: #{d_id}, O: #{no_o_id}, time: #{datetime.to_s}")
      end

      @con.commit
    end

    tx.before_each do
      @input = Hash.new
      @input[:w_id] = home_w_id
      @input[:o_carrier_id] = random_number(1, DIST_PER_WARE)
    end

    tx.after_each { sleep(@think_time["delivery"] || 0) }

    tx.whenever_sqlerror do
      @con.rollback
      log.debug(YAML.dump(@input).chomp)
    end

    return tx
  end

  # Stock-Level Transaction
  def slev
    tx = RTA::Transaction.new("Stock-Level") do
      sql1 = "SELECT d_next_o_id FROM district " +
             "WHERE d_w_id = ? AND d_id = ?"
      stmt1 = @con.prepareStatement(sql1)
      stmt1.setInt(1, @input[:w_id])
      stmt1.setInt(2, @input[:d_id])
      rset = stmt1.executeQuery
      rownum = 0
      while rset.next
        o_id = rset.getInt(1)
        rownum += 1
      end
      rset.close
      stmt1.close
      raise NOT_FOUND_SQL_EXCEPTION if rownum == 0

      sql2 = "SELECT COUNT(DISTINCT (s_i_id)) FROM order_line, stock " +
             "WHERE ol_w_id = ? AND " +
             "  ol_d_id = ? AND  ol_o_id < ? AND " +
             "  ol_o_id >= ? - 20 AND s_w_id = ? AND " + 
             "  s_i_id = ol_i_id AND s_quantity < ?"
      stmt2 = @con.prepareStatement(sql2)
      stmt2.setInt(1, @input[:w_id])
      stmt2.setInt(2, @input[:d_id])
      stmt2.setInt(3, o_id)
      stmt2.setInt(4, o_id)
      stmt2.setInt(5, @input[:w_id])
      stmt2.setInt(6, @input[:threshold])
      rset = stmt2.executeQuery
      while rset.next
        stock_count = rset.getInt(1)
      end
      rset.close
      stmt2.close

      @con.commit
    end

    tx.before_each do
      @input = Hash.new
      @input[:w_id] = home_w_id
      @input[:d_id] = (@session_id - 1) / count_ware + 1
      @input[:threshold] = random_number(10, 20)
    end

    tx.after_each { sleep(@think_time["stock_level"] || 0) }

    tx.whenever_sqlerror do
      @con.rollback
      log.debug(YAML.dump(@input).chomp)
    end

    return tx
  end

  def home_w_id
    return ((@session_id - 1) % count_ware) + @first_w_id
  end

  def count_ware
    return @last_w_id - @first_w_id + 1
  end
end
