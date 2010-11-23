require 'java'
require 'thread'
import java.sql.DriverManager
import Java::oracle.jdbc.driver.OracleDriver
require 'drb'

module RTA
  class Session
    STANDBY = :standby
    GO = :go
    STOP = :stop

    # セッションID
    # @return [Number]
    attr_reader :session_id

    attr_reader :status

    attr_accessor :log

    # セッションを生成
    # 
    # @param [Number] sid セッションID
    # @param [RTA::Log] log ログ
    # @return [Session]
    def initialize(sid, log = RTA::Log.new)
      @session_id = sid
      @status = STANDBY
      @log = log
    end

    def run
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
                  "errmsg: \:#{tx.sql_exception.getMessage}\""
            @log.error(msg)
          end
          msg = "sid: #{@session_id}, " +
                tx.to_s
          @log.debug(msg)
        end
      end

      @log.info("sid: #{@session_id}, msg: \"Session terminated\"")
      @log.info(summary)
      teardown
    end

    def standby
      @status = STANDBY
    end

    def go
      @status = GO
    end

    def stop
      @status = STOP
    end

    def each_transaction
      txs = Array.new
      instance_variables.each do |var|
        klass = instance_variable_get(var).class
        next if klass != RTA::Transaction
        txs << instance_variable_get(var)
      end
      txs.each do |tx|
        yield tx
      end
    end

    def summary
      msgs = Array.new
      each_transaction do |tx|
        msgs << "sid: #{@session_id}, " +
                tx.to_s
      end
      return msgs.join("\n")
    end

    private
    def setup
    end

    def transaction
    end

    def teardown
    end
  end

  class SessionManager
    def initialize(numses, session_class = RTA::Session)
      @sessions = Array.new
      @threads = Array.new
      numses.times do |i|
        @sessions << session_class.new(i + 1)
      end
    end

    def run
      @sessions.each_index do |i|
        @threads[i] ||= Thread.new do
          @sessions[i].run
        end
      end
    end

    def start_service(port)
      DRb.start_service("druby://localhost:#{port}", self)
      DRb.thread.join
    end

    def stop_service
      DRb.stop_service
    end
 
    def standby(sids = nil)
      sids ||= 1 .. @sessions.size
      Array(sids).each do |sid|
        session(sid).standby
      end
    end

    def go(sids = nil)
      sids ||= 1 .. @sessions.size
      Array(sids).each do |sid|
        session(sid).go
      end
    end

    def stop(sids = nil)
      sids ||= 1 .. @sessions.size
      Array(sids).each do |sid|
        session(sid).stop
      end
      if stop_session_count == @sessions.size
        @threads.each { |th| th.join }
        stop_service
      end
    end

    def session(sid)
      return @sessions[sid - 1]
    end

    def thread(sid)
      return @threads[sid - 1]
    end

    def each
      @sessions.each do |ses|
        yield ses
      end
    end

    def each_with_id
      @sessions.each_with_index do |ses, i|
        yield ses, i + 1
      end
    end

    def stop_session_count
      count = 0
      each do |ses|
        count += 1 if ses.status == RTA::Session::STOP
      end
      return count
    end
  end
end
