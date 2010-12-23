require 'yaml'
require 'thread'

class TPCB < RTA::Session
  TPCB_HOME = File.dirname(__FILE__)
  ACCOUNTS_PER_BRANCH = 100000
  TELLERS_PER_BRANCH = 10

  LOCAL_BRANCH_PERCENT = 85

  UPDATE_ACCOUNT_SQL = "UPDATE account " +
                       "SET account_balance = account_balance + ? " +
                       "WHERE account_id = ?"
  SELECT_ACCOUNT_SQL = "SELECT account_balance " +
                       "FROM account " +
                       "WHERE account_id = ?"
  UPDATE_TELLER_SQL = "UPDATE teller " +
                      "SET teller_balance = teller_balance + ? " +
                      "WHERE teller_id = ?"
  UPDATE_BRANCH_SQL = "UPDATE branch " +
                      "SET branch_balance = branch_balance + ? " +
                      "WHERE branch_id = ?"
  INSERT_HISTORY_SQL = "INSERT INTO history VALUES " +
                       "(?, ?, ?, ?, SYSTIMESTAMP, ?)"

  @@mutex = Mutex.new
  @@truncate_history = false
  @@time_str = Time.now.strftime("%Y%m%d%H%M%S")

  def setup
    # ログ
    self.log = RTA::Log.new(TPCB_HOME + "/log/tpcb_#{@@time_str}.log")
    # self.log.level = RTA::Log::DEBUG

    # config.yml の例
    # --
    # Configuration for load and tpcb script
    #
    # tpcb_user: tpcb
    # tpcb_password: tpcb
    # tpcb_url: jdbc:oracle:thin:@192.168.1.5:1521:XE
    # branch_range: 1..10
    config = Hash.new
    File.open(TPCB_HOME + '/config/config.yml', 'r') do |file|
      config = YAML.load(file.read)
    end
    branch_range = eval(config["branch_range"])
    @last_branch_id = branch_range.last

    # 接続
    begin
      # java.lang.Class.forName("oracle.jdbc.driver.OracleDriver")
      @con = DriverManager.getConnection(config["tpcb_url"],
               config["tpcb_user"], config["tpcb_password"])
      @con.setAutoCommit(false)
    rescue SQLException => e
      e.cause.printStackTrace
    rescue ClassNotFoundException => e
      e.cause.printStackTrace
    end

    # Truncate history table
    @@mutex.synchronize do
      unless @@truncate_history
        stmt = @con.createStatement
        stmt.executeUpdate("TRUNCATE TABLE history")
        stmt.close
        self.log.info("Truncate history")
        @@truncate_history = true
      end
    end

    # prepare SQL
    @update_account_stmt = @con.prepareStatement(UPDATE_ACCOUNT_SQL)
    @select_account_stmt = @con.prepareStatement(SELECT_ACCOUNT_SQL)
    @update_teller_stmt = @con.prepareStatement(UPDATE_TELLER_SQL)
    @update_branch_stmt = @con.prepareStatement(UPDATE_BRANCH_SQL)
    @insert_history_stmt = @con.prepareStatement(INSERT_HISTORY_SQL)

    # トランザクション
    @tx_tpcb = RTA::Transaction.new("tpcb") do
      @update_account_stmt.setInt(1, @amount)
      @update_account_stmt.setInt(2, @account_id)
      @update_account_stmt.executeUpdate

      @select_account_stmt.setInt(1, @account_id)
      result_set = @select_account_stmt.executeQuery
      while result_set.next
        @account_balance = result_set.getInt(1)
      end
      result_set.close

      @update_teller_stmt.setInt(1, @amount)
      @update_teller_stmt.setInt(2, @teller_id)
      @update_teller_stmt.executeUpdate

      @update_branch_stmt.setInt(1, @amount)
      @update_branch_stmt.setInt(2, @branch_id)
      @update_branch_stmt.executeUpdate

      @insert_history_stmt.setInt(1, @teller_id)
      @insert_history_stmt.setInt(2, @branch_id)
      @insert_history_stmt.setInt(3, @account_id)
      @insert_history_stmt.setInt(4, @amount)
      info = sprintf("%5d 12345678901", @session_id)
      @insert_history_stmt.setString(5, info)
      @insert_history_stmt.executeUpdate

      @con.commit
    end

    @tx_tpcb.before_each do
      @amount = rand(1999999) - 999999
      @teller_id = rand(@last_branch_id * TELLERS_PER_BRANCH) + 1
      @branch_id = ((@teller_id - 1) / TELLERS_PER_BRANCH) + 1

      account_branch = @branch_id
      if @last_branch_id > 1
        if rand(100) >= LOCAL_BRANCH_PERCENT
          until account_branch != @branch_id
            account_branch = rand(@last_branch_id) + 1
          end
        end
      end

      @account_id = ACCOUNTS_PER_BRANCH * (account_branch - 1) + rand(ACCOUNTS_PER_BRANCH) + 1
    end

    @tx_tpcb.whenever_sqlerror { @con.rollback }
  end

  def transaction
    return @tx_tpcb
  end

  def teardown
    @update_account_stmt.close
    @select_account_stmt.close
    @update_teller_stmt.close
    @update_branch_stmt.close
    @insert_history_stmt.close
    @con.close
  end
end
