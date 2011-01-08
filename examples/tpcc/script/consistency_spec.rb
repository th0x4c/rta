# Usage: jruby -S spec -c -fs consistency_spec.rb

require File.dirname(__FILE__) + '/../script/helper'
require 'java'
import java.sql.DriverManager
import java.sql.SQLException
import Java::oracle.jdbc.driver.OracleDriver
require 'yaml'

describe "TPC-C Consistency Requirements" do
  before(:all) do
    config = Hash.new
    File.open(TPCCHelper::TPCC_HOME + '/config/config.yml', 'r') do |file|
      config = YAML.load(file.read)
    end
    @warehouse_range = eval(config["warehouse_range"])

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
  end

  it "Condition 1: W_YTD = sum(D_YTD)" do
    @warehouse_range.each do |w_id|
      sql1 = "SELECT w_ytd FROM warehouse WHERE w_id = ?"
      stmt1 = @con.prepareStatement(sql1)
      stmt1.setInt(1, w_id)
      rset = stmt1.executeQuery
      while rset.next
        w_ytd = rset.getDouble(1)
      end
      rset.close
      stmt1.close

      sql2 = "SELECT SUM(d_ytd) FROM district WHERE d_w_id = ?"
      stmt2 = @con.prepareStatement(sql2)
      stmt2.setInt(1, w_id)
      rset = stmt2.executeQuery
      while rset.next
        sum_d_ytd = rset.getDouble(1)
      end
      rset.close
      stmt2.close

      w_ytd.should == sum_d_ytd
    end
  end

  it "Condition 2: D_NEXT_O_ID - 1 = max(O_ID) = max(NO_O_ID)" do
    @warehouse_range.each do |w_id|
      1.upto(TPCCHelper::DIST_PER_WARE) do |d_id|
        sql1 = "SELECT d_next_o_id FROM district " +
               "WHERE d_w_id = ? AND d_id = ?"
        stmt1 = @con.prepareStatement(sql1)
        stmt1.setInt(1, w_id)
        stmt1.setInt(2, d_id)
        rset = stmt1.executeQuery
        while rset.next
          d_next_o_id = rset.getInt(1)
        end
        rset.close
        stmt1.close

        sql2 = "SELECT MAX(o_id) FROM orders " +
               "WHERE o_w_id = ? AND o_d_id = ?"
        stmt2 = @con.prepareStatement(sql2)
        stmt2.setInt(1, w_id)
        stmt2.setInt(2, d_id)
        rset = stmt2.executeQuery
        while rset.next
          max_o_id = rset.getInt(1)
        end
        rset.close
        stmt2.close

        sql3 = "SELECT MAX(no_o_id) FROM new_order " +
               "WHERE no_w_id = ? AND no_d_id = ?"
        stmt3 = @con.prepareStatement(sql3)
        stmt3.setInt(1, w_id)
        stmt3.setInt(2, d_id)
        rset = stmt3.executeQuery
        while rset.next
          max_no_id = rset.getInt(1)
        end
        rset.close
        stmt3.close

        (d_next_o_id - 1).should == max_o_id
        (d_next_o_id - 1).should == max_no_id
      end
    end
  end

  it "Condition 3: max(NO_O_ID) - min(NO_O_ID) + 1 = [number of rows in the NEW-ORDER table for this district]" do
    @warehouse_range.each do |w_id|
      1.upto(TPCCHelper::DIST_PER_WARE) do |d_id|
        sql1 = "SELECT MAX(no_o_id) FROM new_order " +
               "WHERE no_w_id = ? AND no_d_id = ?"
        stmt1 = @con.prepareStatement(sql1)
        stmt1.setInt(1, w_id)
        stmt1.setInt(2, d_id)
        rset = stmt1.executeQuery
        while rset.next
          max_no_o_id = rset.getInt(1)
        end
        rset.close
        stmt1.close

        sql2 = "SELECT MIN(no_o_id) FROM new_order " +
               "WHERE no_w_id = ? AND no_d_id = ?"
        stmt2 = @con.prepareStatement(sql2)
        stmt2.setInt(1, w_id)
        stmt2.setInt(2, d_id)
        rset = stmt2.executeQuery
        while rset.next
          min_no_o_id = rset.getInt(1)
        end
        rset.close
        stmt2.close

        sql3 = "SELECT COUNT(*) FROM new_order " +
               "WHERE no_w_id = ? AND no_d_id = ?"
        stmt3 = @con.prepareStatement(sql3)
        stmt3.setInt(1, w_id)
        stmt3.setInt(2, d_id)
        rset = stmt3.executeQuery
        while rset.next
          number_of_rows = rset.getInt(1)
        end
        rset.close
        stmt3.close

        if number_of_rows > 0
          (max_no_o_id - min_no_o_id + 1).should == number_of_rows
        end
      end
    end
  end

  it "Condition 4: sum(O_OL_CNT) = [number of rows in the ORDER-LINE table for this district]" do
    @warehouse_range.each do |w_id|
      1.upto(TPCCHelper::DIST_PER_WARE) do |d_id|
        sql1 = "SELECT SUM(o_ol_cnt) FROM orders " +
               "WHERE o_w_id = ? AND o_d_id = ?"
        stmt1 = @con.prepareStatement(sql1)
        stmt1.setInt(1, w_id)
        stmt1.setInt(2, d_id)
        rset = stmt1.executeQuery
        while rset.next
          sum_o_ol_cnt = rset.getInt(1)
        end
        rset.close
        stmt1.close

        sql2 = "SELECT COUNT(*) FROM order_line " +
               "WHERE ol_w_id = ? AND ol_d_id = ?"
        stmt2 = @con.prepareStatement(sql2)
        stmt2.setInt(1, w_id)
        stmt2.setInt(2, d_id)
        rset = stmt2.executeQuery
        while rset.next
          number_of_rows = rset.getInt(1)
        end
        rset.close
        stmt2.close

        sum_o_ol_cnt.should == number_of_rows
      end
    end
  end

  it "Condition 5: For any row in the ORDER table, O_CARRIER_ID is set to a null value if and only if there is a corresponding row in the NEW-ORDER table" do
    sql1 = "SELECT o_w_id, o_d_id, o_id, o_carrier_id FROM orders"
    stmt1 = @con.prepareStatement(sql1)
    rset1 = stmt1.executeQuery
    while rset1.next
      o_w_id = rset1.getInt("o_w_id")
      o_d_id = rset1.getInt("o_d_id")
      o_id = rset1.getInt("o_id")
      o_carrier_id = rset1.getInt("o_carrier_id")
      o_carrier_id = nil if rset1.wasNull

      sql2 = "SELECT COUNT(*) FROM new_order " +
             "WHERE no_w_id = ? AND no_d_id = ? AND no_o_id = ?"
      stmt2 = @con.prepareStatement(sql2)
      stmt2.setInt(1, o_w_id)
      stmt2.setInt(2, o_d_id)
      stmt2.setInt(3, o_id)
      rset2 = stmt2.executeQuery
      while rset2.next
        number_of_rows = rset2.getInt(1)
      end
      rset2.close
      stmt2.close

      if number_of_rows == 0
        o_carrier_id.should_not be_nil
      else
        o_carrier_id.should be_nil
      end
    end
    rset1.close
    stmt1.close
  end

  it "Condition 6: For any row in the ORDER table, O_OL_CNT must equal the number of rows in the ORDER-LINE table for the corresponding order" do
    sql1 = "SELECT o_w_id, o_d_id, o_id, o_ol_cnt FROM orders"
    stmt1 = @con.prepareStatement(sql1)
    rset1 = stmt1.executeQuery
    while rset1.next
      o_w_id = rset1.getInt("o_w_id")
      o_d_id = rset1.getInt("o_d_id")
      o_id = rset1.getInt("o_id")
      o_ol_cnt = rset1.getInt("o_ol_cnt")

      sql2 = "SELECT COUNT(*) FROM order_line " +
             "WHERE ol_w_id = ? AND ol_d_id = ? AND ol_o_id = ?"
      stmt2 = @con.prepareStatement(sql2)
      stmt2.setInt(1, o_w_id)
      stmt2.setInt(2, o_d_id)
      stmt2.setInt(3, o_id)
      rset2 = stmt2.executeQuery
      while rset2.next
        number_of_rows = rset2.getInt(1)
      end
      rset2.close
      stmt2.close

      o_ol_cnt.should == number_of_rows
    end
    rset1.close
    stmt1.close
  end

  it "Condition 7: For any row in the ORDER-LINE table, OL_DELIVERY_D is set to a null date/time if and only if the corresponding row in the ORDER table has O_CARRIER_ID set to a null value" do
    sql1 = "SELECT ol_w_id, ol_d_id, ol_o_id, ol_delivery_d FROM order_line"
    stmt1 = @con.prepareStatement(sql1)
    rset1 = stmt1.executeQuery
    while rset1.next
      ol_w_id = rset1.getInt("ol_w_id")
      ol_d_id = rset1.getInt("ol_d_id")
      ol_o_id = rset1.getInt("ol_o_id")
      ol_delivery_d = rset1.getTimestamp("ol_delivery_d")
      ol_delivery_d = nil if rset1.wasNull

      sql2 = "SELECT o_carrier_id FROM orders " +
             "WHERE o_w_id = ? AND o_d_id = ? AND o_id = ?"
      stmt2 = @con.prepareStatement(sql2)
      stmt2.setInt(1, ol_w_id)
      stmt2.setInt(2, ol_d_id)
      stmt2.setInt(3, ol_o_id)
      rset2 = stmt2.executeQuery
      while rset2.next
        o_carrier_id = rset2.getInt("o_carrier_id")
        o_carrier_id = nil if rset2.wasNull
      end
      rset2.close
      stmt2.close

      if o_carrier_id.nil?
        ol_delivery_d.should be_nil
      else
        ol_delivery_d.should_not be_nil
      end
    end
    rset1.close
    stmt1.close
  end

  it "Condition 8: W_YTD = sum(H_AMOUNT)" do
    @warehouse_range.each do |w_id|
      sql1 = "SELECT w_ytd FROM warehouse WHERE w_id = ?"
      stmt1 = @con.prepareStatement(sql1)
      stmt1.setInt(1, w_id)
      rset = stmt1.executeQuery
      while rset.next
        w_ytd = rset.getDouble(1)
      end
      rset.close
      stmt1.close

      sql2 = "SELECT SUM(h_amount) FROM history WHERE h_w_id = ?"
      stmt2 = @con.prepareStatement(sql2)
      stmt2.setInt(1, w_id)
      rset = stmt2.executeQuery
      while rset.next
        sum_h_amount = rset.getDouble(1)
      end
      rset.close
      stmt2.close

      w_ytd.should == sum_h_amount
    end
  end

  it "Condition 9: D_YTD = sum(H_AMOUNT)" do
    @warehouse_range.each do |w_id|
      1.upto(TPCCHelper::DIST_PER_WARE) do |d_id|
        sql1 = "SELECT d_ytd FROM district " +
               "WHERE d_w_id = ? AND d_id = ?"
        stmt1 = @con.prepareStatement(sql1)
        stmt1.setInt(1, w_id)
        stmt1.setInt(2, d_id)
        rset = stmt1.executeQuery
        while rset.next
          d_ytd = rset.getDouble(1)
        end
        rset.close
        stmt1.close

        sql2 = "SELECT SUM(h_amount) FROM history " +
               "WHERE h_w_id = ? AND h_d_id = ?"
        stmt2 = @con.prepareStatement(sql2)
        stmt2.setInt(1, w_id)
        stmt2.setInt(2, d_id)
        rset = stmt2.executeQuery
        while rset.next
          sum_h_amount = rset.getDouble(1)
        end
        rset.close
        stmt2.close

        d_ytd.should == sum_h_amount
      end
    end
  end

  it "Condition 10: C_BALANCE = sum(OL_AMOUNT) - sum(H_AMOUNT)" do
    sql1 = "SELECT c_w_id, c_d_id, c_id, c_balance FROM customer"
    stmt1 = @con.prepareStatement(sql1)
    rset1 = stmt1.executeQuery
    while rset1.next
      c_w_id = rset1.getInt("c_w_id")
      c_d_id = rset1.getInt("c_d_id")
      c_id = rset1.getInt("c_id")
      c_balance = rset1.getDouble("c_balance")

      sql2 = "SELECT SUM(ol_amount) FROM order_line, orders " +
             "WHERE ol_w_id = o_w_id AND ol_d_id = o_d_id AND ol_o_id = o_id AND " +
             "  o_w_id = ? AND o_d_id = ? AND o_c_id = ? AND " +
             "  ol_delivery_d IS NOT NULL"
      stmt2 = @con.prepareStatement(sql2)
      stmt2.setInt(1, c_w_id)
      stmt2.setInt(2, c_d_id)
      stmt2.setInt(3, c_id)
      rset2 = stmt2.executeQuery
      while rset2.next
        sum_ol_amount = rset2.getDouble(1)
      end
      rset2.close
      stmt2.close

      sql3 = "SELECT SUM(h_amount) FROM history " +
             "WHERE h_c_w_id = ? AND h_c_d_id = ? AND h_c_id = ?"
      stmt3 = @con.prepareStatement(sql3)
      stmt3.setInt(1, c_w_id)
      stmt3.setInt(2, c_d_id)
      stmt3.setInt(3, c_id)
      rset3 = stmt3.executeQuery
      while rset3.next
        sum_h_amount = rset3.getDouble(1)
      end
      rset3.close
      stmt3.close

      c_balance.should == sum_ol_amount - sum_h_amount
    end
    rset1.close
    stmt1.close
  end

  it "Condition 11: (count(*) from ORDER) - (count(*) from NEW-ORDER) = 2100" do
    @warehouse_range.each do |w_id|
      1.upto(TPCCHelper::DIST_PER_WARE) do |d_id|
        sql1 = "SELECT COUNT(*) FROM orders " +
               "WHERE o_w_id = ? AND o_d_id = ?"
        stmt1 = @con.prepareStatement(sql1)
        stmt1.setInt(1, w_id)
        stmt1.setInt(2, d_id)
        rset = stmt1.executeQuery
        while rset.next
          num_of_orders = rset.getInt(1)
        end
        rset.close
        stmt1.close

        sql2 = "SELECT COUNT(*) FROM new_order " +
               "WHERE no_w_id = ? AND no_d_id = ?"
        stmt2 = @con.prepareStatement(sql2)
        stmt2.setInt(1, w_id)
        stmt2.setInt(2, d_id)
        rset = stmt2.executeQuery
        while rset.next
          num_of_new_orders = rset.getInt(1)
        end
        rset.close
        stmt2.close

        (num_of_orders - num_of_new_orders).should == 2100
      end
    end
  end

  it "Condition 12: C_BALANCE + C_YTD_PAYMENT = sum(OL_AMOUNT)" do
    @warehouse_range.each do |w_id|
      1.upto(TPCCHelper::DIST_PER_WARE) do |d_id|
        c_id = rand(TPCCHelper::CUST_PER_DIST) + 1

        sql1 = "SELECT c_balance, c_ytd_payment FROM customer " +
               "WHERE c_w_id = ? AND c_d_id = ? AND c_id = ?"
        stmt1 = @con.prepareStatement(sql1)
        stmt1.setInt(1, w_id)
        stmt1.setInt(2, d_id)
        stmt1.setInt(3, c_id)
        rset = stmt1.executeQuery
        while rset.next
          c_balance = rset.getDouble("c_balance")
          c_ytd_payment = rset.getDouble("c_ytd_payment")
        end
        rset.close
        stmt1.close

        sql2 = "SELECT sum(ol_amount) FROM order_line, orders " +
               "WHERE ol_w_id = o_w_id AND ol_d_id = o_d_id AND ol_o_id = o_id AND "+
               "  o_w_id = ? AND o_d_id = ? AND o_c_id = ? AND " +
               "  ol_delivery_d IS NOT NULL"
        stmt2 = @con.prepareStatement(sql2)
        stmt2.setInt(1, w_id)
        stmt2.setInt(2, d_id)
        stmt2.setInt(3, c_id)
        rset = stmt2.executeQuery
        while rset.next
          sum_ol_amount = rset.getDouble(1)
        end
        rset.close
        stmt2.close

        (c_balance + c_ytd_payment).should == sum_ol_amount
      end
    end
  end
end
