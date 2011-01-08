require 'java'
require 'thread'
import java.sql.DriverManager
import Java::oracle.jdbc.driver.OracleDriver
require 'drb'

module RTA

  # {Session}, {SessionManager} 用のヘルパーメソッド集
  module SessionHelper
    # 保持する {Transaction} すべてを集計した {TransactionStatistic} を返す.
    # 保持するすべての {Transaction} インスタンスを返す #transactions メソッドが
    # 必要.
    #
    # @return [TransactionStatistic]
    def statistic
      stat = RTA::TransactionStatistic.new
      self.transactions.each do |tx|
        stat += tx.stat
      end
      stat.name = self.stat_name if self.respond_to?(:stat_name)
      return stat
    end
    alias_method :stat, :statistic

    # 統計情報を表す文字列
    #
    # @return [String]
    def to_s
      return statistic.to_s
    end
  end

  # セッションを表すクラス
  class Session
    include SessionHelper

    STANDBY = :standby # スタンバイ状態
    GO = :go # トランザクション実行
    STOP = :stop # 処理を停止

    # {Session} クラス全体で使用する +Mutex+ ロック
    @@semaphore = Mutex.new

    # すべての {Session} インスタンスの配列
    @@sessions = Array.new

    # #run メソッド実行中のインスタンス数
    @@running = 0

    # セッションID
    # @return [Number]
    attr_reader :session_id

    # ステータス
    # @return [Symbol]
    # @see STANDBY
    # @see GO
    # @see STOP
    attr_reader :status

    # ログ
    # @return [Log]
    # @see Log
    attr_accessor :log

    # セッションを生成
    # 
    # @param [Number] sid セッションID
    # @param [Log] log ログ
    # @return [Session]
    def initialize(sid, log = RTA::Log.new)
      @session_id = sid
      @status = STANDBY
      @log = log
      @@semaphore.synchronize do
        @@sessions[sid - 1] = self
      end
    end

    # ステータスが {STOP} になるまで処理を実行.
    # ステータスによってトランザクション実行.
    #
    # @see #status
    # @see #standby
    # @see #go
    # @see #stop
    def run
      @@semaphore.synchronize do
        @@running += 1
        setup_first if @@running == 1
      end
      setup
      standby_msg = false
      start_msg = false
      while @status == GO || @status == STANDBY
        case @status
        when STANDBY
          unless standby_msg
            standby_msg = true
            start_msg = false
            @log.info("sid: #{@session_id}, msg: \"Session is standing by\"")
          end
          sleep 0.01
        when GO
          unless start_msg
            standby_msg = false
            start_msg = true
            @log.info("sid: #{@session_id}, msg: \"Session started\"")
          end

          tx = transaction
          tx.execute
          error = tx.sql_exception ? tx.sql_exception.getErrorCode : 0
          if error != 0
            msg = "sid: #{@session_id}, " +
                  "tx: \"#{tx.name}\", " +
                  "error: #{error}, " +
                  "errmsg: \"#{tx.sql_exception.getMessage.chomp}\""
            @log.error(msg)
          end
          msg = "sid: #{@session_id}, " + tx.to_s
          @log.debug(msg)
        end
      end

      @log.info("sid: #{@session_id}, msg: \"Session terminated\"")
      @log.info(summary)
      teardown
      @@semaphore.synchronize do
        @@running -= 1
        teardown_last if @@running == 0
      end
    end

    # ステータスを {STANDBY} 状態に変更
    # 
    # @see #status
    # @see STANDBY
    def standby
      @status = STANDBY
    end

    # ステータスを {GO} 状態に変更
    # 
    # @see #status
    # @see GO
    def go
      @status = GO
    end

    # ステータスを {STOP} 状態に変更
    # 
    # @see #status
    # @see STOP
    def stop
      @status = STOP
    end

    # 保持する {Transaction} インスタンスを配列にして返す
    # 
    # @return [Array<Transaction>]
    def transactions
      txs = Array.new
      instance_variables.each do |var|
        klass = instance_variable_get(var).class
        next if klass != RTA::Transaction
        txs << instance_variable_get(var)
      end
      return txs
    end

    # 統計情報出力の際に使用するセッション名
    # 
    # @return [String]
    def stat_name
      return "All SID #{@session_id} TXs"
    end

    # 統計情報のサマリーを表す文字列
    # 
    # @return [String]
    def summary
      msgs = Array.new
      msgs << "sid: #{@session_id}, " + self.to_s
      transactions.each do |tx|
        msgs << "sid: #{@session_id}, " + tx.to_s
      end
      return msgs.join("\n")
    end

    # すべての {Session} インスタンスの配列
    #
    # @return [Array<Session>] すべての {Session} インスタンスの配列
    def sessions
      return @@sessions
    end

    private
    # すべての {Session} インスタンス中で最初のセッション開始時に1度だけ
    # 実行される処理.
    # 継承したクラスで実装.
    def setup_first
    end

    # セッション開始時に1度だけ実行される処理.
    # 継承したクラスで実装.
    def setup
    end

    # 実行する {Transaction} インスタンスを返す.
    # 継承したクラスで実装する必要がある.
    #
    # @return [Transaction]
    def transaction
    end

    # セッション終了時に1度だけ実行される処理.
    # 継承したクラスで実装.
    def teardown
    end

    # すべての {Session} インスタンス中で最後のセッション終了時に1度だけ
    # 実行される処理.
    # 継承したクラスで実装.
    def teardown_last
    end
  end

  # セッションを管理するクラス
  class SessionManager
    include SessionHelper

    # {SessionManager} インスタンスを生成.
    # 渡されたセッション数の分だけ {Session} のサブクラスのインスタンスを生成.
    #
    # @param [Number] numses セッション数
    # @param [Class] session_class 生成する +Session+ サブクラス名
    # @return [SessionManager]
    def initialize(numses, session_class = RTA::Session)
      @sessions = Array.new
      @threads = Array.new
      numses.times do |i|
        @sessions << session_class.new(i + 1)
      end
    end

    # すべてのセッションの処理開始
    def run
      @sessions.each_index do |i|
        @threads[i] ||= Thread.new do
          @sessions[i].run
        end
      end
    end

    # すべてのセッションの終了を待つ
    def wait
      @threads.each { |th| th.join }
    end

    # 分散オブジェクト(dRuby) のサービス開始
    #
    # @param [Number] port ポート番号
    def start_service(port)
      DRb.start_service("druby://localhost:#{port}", self)
      wait
      stop_service
    end

    # 分散オブジェクト(dRuby) のサービス停止
    def stop_service
      DRb.stop_service
    end
 
    # 指定されたセッションのステータスを {Session::STANDBY} にする.
    # 指定なしの場合はすべてのセッションのステータスを変更.
    #
    # @param [Array<Number>] sids ステータスを変更するセッションの ID
    def standby(sids = nil)
      sids ||= 1 .. @sessions.size
      Array(sids).each do |sid|
        session(sid).standby
      end
    end

    # 指定されたセッションのステータスを {Session::GO} にする.
    # 指定なしの場合はすべてのセッションのステータスを変更.
    #
    # @param [Array<Number>] sids ステータスを変更するセッションの ID
    def go(sids = nil)
      sids ||= 1 .. @sessions.size
      Array(sids).each do |sid|
        session(sid).go
      end
    end

    # 指定されたセッションのステータスを {Session::STOP} にする.
    # 指定なしの場合はすべてのセッションのステータスを変更.
    #
    # @param [Array<Number>] sids ステータスを変更するセッションの ID
    def stop(sids = nil)
      sids ||= 1 .. @sessions.size
      Array(sids).each do |sid|
        session(sid).stop
      end
    end

    # 指定されたセッションの {Session} インスタンスを返す
    #
    # @param [Number] sid セッション ID
    # @return [Session]
    def session(sid)
      return @sessions[sid - 1]
    end

    # 指定されたセッションの +Thread+ インスタンスを返す
    #
    # @param [Number] sid セッション ID
    # @return [Thread]
    def thread(sid)
      return @threads[sid - 1]
    end

    # すべてのセッションに対して与えられた block を実行
    #
    # @yield [ses] セッションに対して実行する処理
    # @yieldparam [Session] ses セッション
    def each
      @sessions.each do |ses|
        yield ses
      end
    end

    # すべてのセッションに対して与えられた block を実行
    #
    # @yield [ses, id] セッションに対して実行する処理
    # @yieldparam [Session] ses セッション
    # @yieldparam [Number] id セッション ID
    def each_with_id
      @sessions.each_with_index do |ses, i|
        yield ses, i + 1
      end
    end

    # {Session::STOP} 状態のセッション数
    #
    # @return [Number]
    def stop_session_count
      count = 0
      each do |ses|
        count += 1 if ses.status == RTA::Session::STOP
      end
      return count
    end

    # すべてのセッションの保持する {Transaction} インスタンスを配列にして返す
    # 
    # @return [Array<Transaction>]
    def transactions
      txs = Array.new
      @sessions.each do |ses|
        txs += ses.transactions
      end
      return txs
    end

    # 統計情報出力の際に使用する名前
    # 
    # @return [String]
    def stat_name
      return "All SID TXs"
    end
  end
end
