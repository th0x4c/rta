# -*- coding: utf-8 -*-
require 'yaml'
require 'thread'
import java.sql.DriverManager
import Java::oracle.jdbc.pool.OracleDataSource

class TPCHTeardown < RTA::Session
  TPCH_HOME = File.dirname(__FILE__) + '/../'

  def setup
    # ログ
    # self.log = RTA::Log.new(TPCH_HOME + "/log/teardown_#{Time.now.strftime("%Y%m%d%H%M%S")}.log")
    # self.log.level = RTA::Log::DEBUG

    config = Hash.new
    File.open(TPCH_HOME + '/config/config.yml', 'r') do |file|
      config = YAML.load(file.read)
    end

    # 接続
    begin
      @ds = OracleDataSource.new
      @ds.setURL(config["tpch_url"])
      @ds.setUser(config["tpch_user"])
      @ds.setPassword(config["tpch_password"])
      @con = @ds.getConnection
      @con.setAutoCommit(false)
    rescue SQLException => e
      e.printStackTrace
    rescue ClassNotFoundException => e
      e.printStackTrace
    end

    @tx_teardown = RTA::Transaction.new("tpch teardown") do
      drop_table(@con)
      drop_external_table(@con)
      drop_directory(@con)
    end

    @tx_last = RTA::Transaction.new("last") do
      @con.commit
    end

    self.go
  end

  def transaction
    if @tx_teardown.count == 0
      return @tx_teardown
    else
      self.stop
      return @tx_last
    end
  end

  private
  def drop_directory(con)
    sql = "DROP DIRECTORY rta_tpch_dir"
    exec_sql(con, sql)
  end

  def drop_external_table(con)
    tables = %w( lineitem_et
                 orders_et
                 partsupp_et
                 part_et
                 customer_et
                 supplier_et
                 nation_et
                 region_et
                 temp_lineitem_et
                 temp_orders_et
                 temp_orderkey_et )

    tables.each do |table|
      sql = "DROP TABLE " + table
      exec_sql(con, sql)
    end
  end

  def drop_table(con)
    tables = %w( lineitem
                 orders
                 partsupp
                 part
                 customer
                 supplier
                 nation
                 region
                 temp_orderkey )

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
