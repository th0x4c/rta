require 'pp'
require 'java'
require '../../lib/rta'
import java.sql.DriverManager
import Java::oracle.jdbc.driver.OracleDriver

class Example
  def getCon
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
  end

  def readData
    tx = RTA::Transaction.new do
      stmt = @con.prepareStatement("select ename,empno from emp")
      rset = stmt.executeQuery
      while rset.next
        puts rset.getString(1) + " " + rset.getString(2)
      end
      rset.close

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
      stmt.close
    end

    tx.before_all do
      @pstmt = @con.prepareStatement("select ename from emp where deptno = ?")
    end


    10.times do
      tx.execute
      puts "[#{tx.elapsed_time}, #{tx.total_elapsed_time}, #{tx.min_elapsed_time}, #{tx.max_elapsed_time}]"
    end
    pp tx
  end

  def closeCon
    if @con != nil
      begin
        @con.close
      rescue SQLException => e
        e.cause.printStackTrace
      end
    end
  end
end

example = Example.new
example.getCon
example.readData
example.closeCon
