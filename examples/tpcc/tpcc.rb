require 'yaml'
require 'thread'
import java.sql.Timestamp

class TPCC < RTA::Session
  TPCC_HOME = File.dirname(__FILE__)
  MAXITEMS = 100000
  CUST_PER_DIST = 3000
  DIST_PER_WARE = 10
  ORD_PER_DIST = 3000

  CNUM = 1

  UNUSED_I_ID = -1

  INVALID_ITEM_ERROR_CODE = -1
  INVALID_ITEM_SQL_EXCEPTION = SQLException.new.initCause(SQLException.new("Item number is not valid", nil, INVALID_ITEM_ERROR_CODE))

  @@mutex = Mutex.new
  @@truncate_history = false
  @@time_str = Time.now.strftime("%Y%m%d%H%M%S")

  def setup
    # ログ
    self.log = RTA::Log.new(TPCC_HOME + "/log/tpcc_#{@@time_str}.log")
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
    @first_w_id = warehouse_range.first
    @last_w_id =  warehouse_range.last
    if @first_w_id <= 0 || @last_w_id <= 0
      log.error("Invalid Warehouse Count.")
      exit(-1)
    end
    @think_time = config["think_time"]
    @tx_percentage = config["tx_percentage"]
    # @tx_percentage を % に変換. (合計が 100 となるようにする)
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
    @tx["new_order"] = @neword = neword    # New-Order Transaction  
    @tx["payment"] = @payment = payment    # Payment Transaction    
    @tx["order_status"] = @ostat = ostat   # Order-Status Transaction
    @tx["delivery"] = @delivery = delivery # Delivery Transaction   
    @tx["stock_level"] = @slev =slev       # Stock-Level Transaction
  end

  def transaction
    rand_pct = rand * 100 # rand returns a pseudorandom floating point number
                          # greater than or equal to 0.0 and less than 1.0
    cul = 0
    @tx.each do |tx_name, tx|
      cul += @tx_percentage[tx_name]
      return tx if rand_pct < cul
    end

    # Unreachable unless all transaction percentages are 0.
    log.fatal("No transaction available")
    raise "No transaction available"
  end

  def teardown
    @con.close
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
      while rset.next
        c_discount = rset.getFloat("c_discount")
        c_last = rset.getString("c_last")
        c_credit = rset.getString("c_credit")
        w_tax = rset.getFloat("w_tax")
      end
      rset.close
      stmt1.close

      sql2 = "SELECT d_next_o_id, d_tax " +
             "FROM district "+
             "WHERE d_id = ? AND d_w_id = ?"
      stmt2 = @con.prepareStatement(sql2)
      stmt2.setInt(1, @input[:d_id])
      stmt2.setInt(2, @input[:w_id])
      rset = stmt2.executeQuery
      while rset.next
        d_next_o_id = rset.getInt("d_next_o_id")
        d_tax = rset.getFloat("d_tax")
      end
      rset.close
      stmt2.close

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
             "  o_entry_d, o_ol_cnt, o_all_local) " +
             "VALUES (?, ?, ?, ?, ?, ?, ?)"
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
        if rownum == 0
          raise INVALID_ITEM_SQL_EXCEPTION
        end

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
        end
        rset.close
        stmt7.close

        ol_dist_info = s_dist_xx[@input[:d_id] - 1] # pick correct s_dist_xx
        stock[ol_number] = s_quantity

        if i_data =~ /original/ && s_data =~ /original/
          bg[ol_number] = 'B'
        else
          bg[ol_number] = 'G'
        end

        if s_quantity > ol_quantity
          s_quantity = s_quantity - ol_quantity
        else
          s_quantity = s_quantity - ol_quantity + 91
        end

        sql8 = "UPDATE stock SET s_quantity = ? " +
               "WHERE s_i_id = ? " +
               "AND s_w_id = ?"
        stmt8 = @con.prepareStatement(sql8)
        stmt8.setInt(1, s_quantity)
        stmt8.setInt(2, ol_i_id)
        stmt8.setInt(3, ol_supply_w_id)
        stmt8.executeUpdate
        stmt8.close

        ol_amount = ol_quantity * price[ol_number] * (1 + w_tax + d_tax) * (1 - c_discount)
        amt[ol_number] = ol_amount
        total += ol_amount

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
    end
    tx.after_each { sleep(@think_time["payment"] || 0) }
    tx.whenever_sqlerror { @con.rollback }
    return tx
  end

  # Order-Status Transaction
  def ostat
    tx = RTA::Transaction.new("Order-Status") do
    end
    tx.after_each { sleep(@think_time["order_status"] || 0) }
    tx.whenever_sqlerror { @con.rollback }
    return tx
  end

  # Delivery Transaction
  def delivery
    tx = RTA::Transaction.new("Delivery") do
    end
    tx.after_each { sleep(@think_time["delivery"] || 0) }
    tx.whenever_sqlerror { @con.rollback }
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
      while rset.next
        o_id = rset.getInt(1)
      end
      rset.close
      stmt1.close

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

    tx.before_all do
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

  def random_number(min, max)
    return min + rand(max - min + 1)
  end

  def nurand(a, x, y)
    return ((((random_number(0, a) | random_number(x, y)) + CNUM) % (y - x + 1)) + x)
  end
end
