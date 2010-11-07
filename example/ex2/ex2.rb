require 'java'
require '../../lib/rta'


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

    @tx1 = RTA::Transaction.new("tx1") do
      stmt = @con.prepareStatement("select ename,empno from emp")
      rset = stmt.executeQuery
      while rset.next
        puts rset.getString(1) + " " + rset.getString(2)
      end
      rset.close
    end
    @tx1.after { sleep 1 }

    @tx2 = RTA::Transaction.new("tx2") do
      [10, 20, 30].each do |dno|
        puts ""
        @pstmt.setInt(1, dno)
        rset = @pstmt.executeQuery
        puts "DEPTNO: " + dno.to_s
        while rset.next
          puts "  " + rset.getString(1)
        end
        rset.close
      end
    end
    @tx2.before_all do
      @pstmt = @con.prepareStatement("select ename from emp where deptno = ?")
    end
  end

  def transaction
    stop if @tx1.count == 5
    if @tx1.count < @tx2.count
      return @tx1
    else
      return @tx2
    end
  end

  def teardown
    @con.close
  end
end

s1 = ExampleSession.new(1, RTA::Log.new("./test.log"))
s1.log.level = RTA::Log::DEBUG
th = Thread.new do
  s1.run
end
sleep 5
s1.go
th.join
