# -*- coding: utf-8 -*-
require 'yaml'
require 'thread'
import java.sql.DriverManager
import Java::oracle.jdbc.driver.OracleDriver

class TPCBLoad < RTA::Session
  TPCB_HOME = File.dirname(__FILE__) + '/../'
  ACCOUNTS_PER_BRANCH = 100000
  TELLERS_PER_BRANCH = 10

  INSERTS_PER_COMMIT = 100

  BRANCH_SQL = "INSERT INTO branch (branch_id, branch_balance, filler) " +
               "VALUES (?, 0, '12345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678')"
  TELLER_SQL = "INSERT INTO teller (teller_id, branch_id, teller_balance, filler) " +
               "VALUES (?, ?, 0,'1234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567')"
  ACCOUNT_SQL = "INSERT INTO account (account_id, branch_id, account_balance, filler) " +
                "VALUES (?, ?, 0, '1234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567')"

  @@mutex = Mutex.new
  @@class_account_id = nil

  def setup
    # config.yml の例
    # --
    # Configuration for load and tpcb script
    #
    # tpcb_user: tpcb
    # tpcb_password: tpcb
    # tpcb_url: jdbc:oracle:thin:@//192.168.1.5:1521/XE
    # branch_range: 1..10
    config = Hash.new
    File.open(TPCB_HOME + '/config/config.yml', 'r') do |file|
      config = YAML.load(file.read)
    end
    branch_range = eval(config["branch_range"])
    @first_account_id = (branch_range.first - 1) * ACCOUNTS_PER_BRANCH + 1
    @last_account_id =  branch_range.last * ACCOUNTS_PER_BRANCH
    @@mutex.synchronize do
        @@class_account_id ||= @first_account_id
    end

    # 接続
    begin
      @con = DriverManager.getConnection(config["tpcb_url"],
               config["tpcb_user"], config["tpcb_password"])
      @con.setAutoCommit(false)
    rescue SQLException => e
      e.printStackTrace
    rescue ClassNotFoundException => e
      e.printStackTrace
    end

    # prepare SQL
    @branch_stmt = @con.prepareStatement(BRANCH_SQL)
    @teller_stmt = @con.prepareStatement(TELLER_SQL)
    @account_stmt = @con.prepareStatement(ACCOUNT_SQL)

    # トランザクション
    @tx_load = RTA::Transaction.new("load") do
      branch_id = (@account_id - 1) / ACCOUNTS_PER_BRANCH + 1
      if @account_id % ACCOUNTS_PER_BRANCH == 1
        @branch_stmt.setInt(1, branch_id)
        @branch_stmt.executeUpdate

        first_teller_id = (branch_id - 1) * TELLERS_PER_BRANCH + 1
        last_teller_id = first_teller_id + TELLERS_PER_BRANCH - 1
        first_teller_id.upto(last_teller_id) do |teller_id|
          @teller_stmt.setInt(1, teller_id)
          @teller_stmt.setInt(2, branch_id)
          @teller_stmt.executeUpdate
        end
      end

      @account_stmt.setInt(1, @account_id)
      @account_stmt.setInt(2, branch_id)
      @account_stmt.executeUpdate

      @con.commit if self.stat.count % INSERTS_PER_COMMIT == 0
    end
    @tx_load.whenever_sqlerror { @con.rollback }

    @tx_last = RTA::Transaction.new("last") do
      @con.commit
    end

    self.go

    # ログ
    # self.log = RTA::Log.new(TPCB_HOME + "/log/load_#{@session_id}.log")
    # self.log.level = RTA::Log::DEBUG
  end

  def transaction
    tx = nil
    @@mutex.synchronize do
      if @@class_account_id <= @last_account_id
        tx = @tx_load
        @account_id = @@class_account_id
        @@class_account_id += 1
      else
        stop
        tx = @tx_last
      end
    end
    return tx
  end

  def teardown
    @con.commit
    @branch_stmt.close
    @teller_stmt.close
    @account_stmt.close
    @con.close
  end
end
