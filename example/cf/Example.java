import java.sql.*;

public class Example
{
  Connection con;
  Statement stmt;
  ResultSet rset;

  Example()
  {
    this.con = null;
    this.stmt = null;
    this.rset = null;
  }

  public void getCon()
  {
    try
    {
      Class.forName("oracle.jdbc.driver.OracleDriver");
//      String url = "jdbc:oracle:oci:@oracle";
      String url = "jdbc:oracle:thin:@192.168.1.5:1521:XE";
      String user = "scott";
      String passwd = "tiger";
      con = DriverManager.getConnection(url,user,passwd);
    }
    catch ( SQLException e )
    {
      e.printStackTrace();
    }
    catch ( ClassNotFoundException e )
    {
      e.printStackTrace();
    }
  }

  public void closeCon()
  {
    if (con != null)
    {
      try
      {
        con.close();
      }
      catch (SQLException e)
      {
        e.printStackTrace();
      }
    }
  }

  public void readData()
  {
    try
    {
      stmt = con.createStatement();
      rset = stmt.executeQuery("select ename,empno from emp");
      while(rset.next())
      {
        System.out.println(rset.getString(1)+ " " + rset.getString(2));
      }
      rset.close();
      stmt.close();
    }
    catch (SQLException e)
    {
      e.printStackTrace();
    }
  }

  public void insData()
  {
    try
    {
      stmt = con.createStatement();
      int status = stmt.executeUpdate("Insert into emp(empno,ename,deptno) values(9999,'JDBC',10)");
      System.out.println("Insert Success and Commited.");
      stmt.close();
    }
    catch (SQLException e)
    {
//    con.rollback();
      System.out.print("No record was inserted .");
      System.out.println(e.getErrorCode() +" = " + e.getMessage());
      e.printStackTrace();
    }
  }

  public void delData()
  {
    try
    {
      stmt = con.createStatement();
      int status = stmt.executeUpdate("delete from emp where empno=9999");
      if(status > 0)
      {
        System.out.println("Delete Success");
      }
      else
      {
        System.out.println("No Data to Delete");
      }
      stmt.close();
    }
    catch (SQLException e)
    {
      e.printStackTrace();
    }
  }

  public static void main(String[] args)
  {
    Example example = new Example();
    example.getCon();
    example.readData();
//    example.insData();
//    example.delData();
    example.closeCon();
  }
}

