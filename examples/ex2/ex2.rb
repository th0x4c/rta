# -*- coding: utf-8 -*-
require 'singleton'
import java.util.Properties
import java.sql.DriverManager
import Java::oracle.jdbc.driver.OracleDriver
import Java::oracle.jdbc.pool.OracleOCIConnectionPool

class ConnectionPool
  include Singleton

  def initialize
    begin
      url = "jdbc:oracle:oci8:@XE"
      user = "scott"
      passwd = "tiger"
      @cpool = OracleOCIConnectionPool.new(user, passwd, url, nil)
      p1 = Properties.new
      p1.put(OracleOCIConnectionPool::CONNPOOL_MIN_LIMIT, "1")
      p1.put(OracleOCIConnectionPool::CONNPOOL_MAX_LIMIT, "3")
      p1.put(OracleOCIConnectionPool::CONNPOOL_INCREMENT, "1")
      @cpool.setPoolConfig(p1)
    rescue SQLException => e
      e.printStackTrace
    rescue ClassNotFoundException => e
      e.printStackTrace
    end
  end

  def getConnection
    return @cpool.getConnection
  end
end

class ExampleSession < RTA::Session
  def setup
    @cpool = ConnectionPool.instance

    # トランザクション
    @tx1 = RTA::Transaction.new("tx1") do
      begin
        @con = @cpool.getConnection
        stmt = @con.prepareStatement("select ename, comm from emp where empno = 7900")
        rset = stmt.executeQuery
        while rset.next
          log.info("sid: #{@session_id} " + rset.getString(1) + " " + rset.getInt(2).to_s)
        end
      ensure
        rset.close if rset
        stmt.close if stmt
        @con.close if @con
      end
    end
    @tx1.after { sleep 1 }

    @tx2 = RTA::Transaction.new("tx2") do
      begin
        @con = @cpool.getConnection
        stmt = @con.createStatement
        stmt.executeUpdate("update emp set comm = #{@session_id} where empno = 7900")
        @con.commit
      ensure
        stmt.close if stmt
        @con.close if @con
      end
    end
    @tx2.after { sleep 0.5 }
    @tx2.whenever_sqlerror { @con.rollback }

    # ログ
    # self.log = RTA::Log.new("./test_#{@session_id}.log")
    # self.log.level = RTA::Log::DEBUG
  end

  def transaction
    # tx1 と tx2 を交互に実行
    count = @tx1.count + @tx2.count
    tx = count % 2 == 0 ? @tx1 : @tx2
    return tx
  end

  def teardown
  end
end
