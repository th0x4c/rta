class ExampleSession < RTA::Session
  def setup
    begin
      # java.lang.Class.forName("oracle.jdbc.driver.OracleDriver")
      url = "jdbc:oracle:thin:@192.168.1.5:1521:XE"
      user = "scott"
      passwd = "tiger"
      @con = DriverManager.getConnection(url, user, passwd)
    rescue SQLException => e
      e.cause.printStackTrace
    rescue ClassNotFoundException => e
      e.cause.printStackTrace
    end

    self.log = RTA::Log.new("./test_#{@session_id}.log")
    self.log.level = RTA::Log::DEBUG
  end

  def transaction
    unless @tx1
      @tx1 = RTA::Transaction.new("tx1") do
        stmt = @con.prepareStatement("select ename,empno from emp")
        rset = stmt.executeQuery
        while rset.next
          puts rset.getString(1) + " " + rset.getString(2)
        end
        rset.close
      end
      @tx1.after { sleep 1 }
    end
    return @tx1
  end

  def teardown
    @con.close
  end
end
