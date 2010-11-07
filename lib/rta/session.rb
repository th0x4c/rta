require 'java'
require 'thread'
import java.sql.DriverManager
import Java::oracle.jdbc.driver.OracleDriver

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
                "tx: \"#{tx.name}\", " +
                "start: \"#{time_to_str(tx.start_time)}\", " +
                "elapsed: #{tx.elapsed_time}, " +
                "error: #{error}"
          @log.debug(msg)
        end
      end

      @log.info("sid: #{@session_id}, msg: \"Session terminated\"")
      summary
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

    def summary
      instance_variables.each do |var|
        klass = instance_variable_get(var).class
        next if klass != RTA::Transaction
        tx = instance_variable_get(var)
        avg = tx.count == 0 ? 0 : tx.total_elapsed_time / tx.count
        msg = "sid: #{@session_id}, " +
              "tx: \"#{tx.name}\", " +
              "count: #{tx.count}, " +
              "error: #{tx.error_count}, " +
              "start: \"#{time_to_str(tx.first_time)}\", " +
              "end: \"#{time_to_str(tx.end_time)}\", " +
              "elapsed: #{tx.total_elapsed_time}, " +
              "avg: #{sprintf("%.3f", avg)}, " +
              "max: #{tx.max_elapsed_time}, " +
              "min: #{tx.min_elapsed_time}"
        @log.info(msg)
      end
    end

    private
    def setup
    end

    def transaction
    end

    def teardown
    end

    def time_to_str(time)
      return time.strftime("%Y-%m-%d %X.") + sprintf("%03d", (time.usec / 1000))
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
 
    def standby(sids = nil)
      sids ||= 1 .. @sessions.size
      Array(sids).each do |sid|
        @sessions[sid - 1].standby
      end
    end

    def go(sids = nil)
      sids ||= 1 .. @sessions.size
      Array(sids).each do |sid|
        @sessions[sid - 1].go
      end
    end

    def stop(sids = nil)
      sids ||= 1 .. @sessions.size
      Array(sids).each do |sid|
        @sessions[sid - 1].stop
        @threads[sid - 1].join
      end
    end

    def session(sid)
      return @sessions[sid - 1]
    end

    def each
      @sessions.each do |ses|
        yield ses
      end
    end
  end
end
