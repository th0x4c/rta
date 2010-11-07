require 'java'
import java.lang.ClassNotFoundException
import java.sql.DriverManager
import java.sql.SQLException
import Java::oracle.jdbc.driver.OracleDriver

class Example
  def initialize
    @con = nil
    @stmt = nil
    @rset = nil
  end

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

  def closeCon
    if @con != nil
      begin
        @con.close
      rescue SQLException => e
        e.cause.printStackTrace
      end
    end
  end

  def readData
    begin
      @stmt = @con.createStatement
      @rset = @stmt.executeQuery("select ename,empno from emp")
      while @rset.next
        puts @rset.getString(1) + " " + @rset.getString(2)
      end
      @rset.close
      @stmt.close
    rescue SQLException => e
      e.cause.printStackTrace
    end
  end
end

example = Example.new
example.getCon
example.readData
example.closeCon
