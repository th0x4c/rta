# -*- coding: utf-8 -*-
require 'yaml'
require 'thread'
import java.sql.DriverManager
import Java::oracle.jdbc.pool.OracleDataSource

require File.dirname(__FILE__) + '/helper'

class TPCCTeardown < RTA::Session
  include TPCCHelper

  def setup
    # ログ
    # self.log = RTA::Log.new(TPCC_HOME + "/log/teardown_#{Time.now.strftime("%Y%m%d%H%M%S")}.log")
    # self.log.level = RTA::Log::DEBUG

    config = Hash.new
    File.open(TPCC_HOME + '/config/config.yml', 'r') do |file|
      config = YAML.load(file.read)
    end

    # 接続
    begin
      @ds = OracleDataSource.new
      @ds.setURL(config["tpcc_url"])
      @ds.setUser(config["tpcc_user"])
      @ds.setPassword(config["tpcc_password"])
      @con = @ds.getConnection
      @con.setAutoCommit(false)
    rescue SQLException => e
      e.printStackTrace
    rescue ClassNotFoundException => e
      e.printStackTrace
    end

    @tx_teardown = RTA::Transaction.new("tpcc teardown") do
      drop_table(@con)
    end

    @tx_last = RTA::Transaction.new("last") do
      @con.commit
    end

    self.go
  end

  def transaction
    if @tx_teardown.count == 0 && @session_id == 1
      return @tx_teardown
    else
      self.stop
      return @tx_last
    end
  end

  private
  def drop_table(con)
    tables = %w( order_line
                 stock
                 item
                 new_order
                 orders
                 history
                 customer
                 district
                 warehouse )

    tables.each do |table|
      sql = "DROP TABLE " + table
      exec_sql(con, sql)
      sql = "PURGE TABLE " + table
      exec_sql(con, sql)
    end
  end

  def exec_sql(con, sql)
    self.log.info(sql_log_str(sql))
    stmt = nil
    begin
      stmt = con.createStatement
      stmt.executeUpdate(sql)
    rescue SQLException => e
      self.log.warn(e.getMessage.chomp)
    ensure
      stmt.close if stmt
    end
  end

  def sql_log_str(sql)
    return ("SQL> " + sql + "\n/").each_line.map { |x| "     " + x }.join.lstrip
  end
end
