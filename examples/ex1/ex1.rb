# -*- coding: utf-8 -*-
import java.sql.DriverManager
import Java::oracle.jdbc.driver.OracleDriver

class ExampleSession < RTA::Session
  def setup
    # 接続
    begin
      url = "jdbc:oracle:thin:@192.168.1.5:1521:XE"
      user = "scott"
      passwd = "tiger"
      @con = DriverManager.getConnection(url, user, passwd)
      @con.setAutoCommit(false)
    rescue SQLException => e
      e.cause.printStackTrace
    rescue ClassNotFoundException => e
      e.cause.printStackTrace
    end

    # トランザクション
    @tx1 = RTA::Transaction.new("tx1") do
      stmt = @con.prepareStatement("select ename, comm from emp where empno = 7900")
      rset = stmt.executeQuery
      while rset.next
        log.info("sid: #{@session_id} " + rset.getString(1) + " " + rset.getInt(2).to_s)
      end
      rset.close
      stmt.close
    end
    @tx1.after { sleep 1 }

    @tx2 = RTA::Transaction.new("tx2") do
      stmt = @con.createStatement
      stmt.executeUpdate("update emp set comm = #{@session_id} where empno = 7900")
      @con.commit
      stmt.close
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
    @con.close
  end
end
