require 'java'
import java.sql.SQLException
require "forwardable"

module RTA
  # {Transaction} の統計情報を表すクラス
  #
  # @see Transaction
  class TransactionStatistic
    # Transaction 名
    # @return [String]
    attr_accessor :name

    # 実行回数
    # @return [Number]
    attr_reader :count

    # 初回実行時の開始時刻
    # @return [Time]
    # @return [nil] まだ1度も実行されていない場合は nil
    attr_reader :first_time

    # 前回実行時の開始時刻
    # @return [Time]
    # @return [nil] まだ1度も実行されていない場合は nil
    attr_reader :start_time

    # 前回実行時の終了時刻
    # @return [Time]
    # @return [nil] まだ1度も実行されていない場合は nil
    attr_reader :end_time

    # 前回の実行時間
    # @return [Float]
    attr_reader :elapsed_time

    # 実行時間の合計
    # @return [Float]
    attr_reader :total_elapsed_time

    # 実行時間の最大
    # @return [Float]
    # @return [nil] まだ1度も実行されていない場合は nil
    attr_reader :max_elapsed_time

    # 実行時間の最小
    # @return [Float]
    # @return [nil] まだ1度も実行されていない場合は nil
    attr_reader :min_elapsed_time

    # エラー回数
    # @return [Number]
    attr_reader :error_count

    # 前回実行時の +SQLException+
    # @return [SQLException]
    # @return [nil] 前回実行時にエラーなしの場合は nil
    attr_reader :sql_exception

    # TransactionStatistic インスタンスを生成
    # 
    # @param [String] name トランザクション名
    # @param [Hash]   stat_hash {TransactionStatistic} を表す +Hash+
    # @return [TransactionStatistic]
    def initialize(name = "",
                   stat_hash = {:name => nil, :count => 0, :first_time => nil,
                                :start_time => nil, :end_time => nil,
                                :elapsed_time => 0, :total_elapsed_time => 0,
                                :max_elapsed_time => nil, :min_elapsed_time => nil,
                                :error_count => 0, :sql_exception => nil})
      @name = stat_hash[:name] || name
      @count = stat_hash[:count]
      @elapsed_time = stat_hash[:elapsed_time]
      @max_elapsed_time = stat_hash[:max_elapsed_time]
      @min_elapsed_time = stat_hash[:min_elapsed_time]
      @total_elapsed_time = stat_hash[:total_elapsed_time]
      @error_count = stat_hash[:error_count]

      @first_time = stat_hash[:first_time]
      @start_time = stat_hash[:start_time]
      @end_time = stat_hash[:end_time]
      @sql_exception = stat_hash[:sql_exception]
    end

    # トランザクション実行時に開始直前に呼ぶ.
    # もし block が渡されていれば, そのブロックを実行.
    # 
    # @yield 実行するトランザクション
    def start
      @sql_exception = nil
      @start_time = Time.now
      @first_time ||= @start_time
      if block_given?
        yield
        self.end
      end
    end

    # +SQLException+ を設定
    # 
    # @param [SQLException] exception トランザクション実行時に発生した +SQLException+
    def sql_exception=(exception)
      @sql_exception = exception
      @error_count += 1
    end

    # トランザクション実行時に終了直後に呼ぶ
    def end
      @end_time = Time.now
      @count += 1
      @elapsed_time = @end_time - @start_time
      @total_elapsed_time += @elapsed_time
      if @count == 1
        @max_elapsed_time = @elapsed_time
        @min_elapsed_time = @elapsed_time
      else
        @max_elapsed_time =
          @elapsed_time > @max_elapsed_time ? @elapsed_time : @max_elapsed_time
        @min_elapsed_time =
          @elapsed_time < @min_elapsed_time ? @elapsed_time : @min_elapsed_time
      end
    end

    # 実行時間の平均
    # 
    # @return [Float] 実行時間の平均
    def avg_elapsed_time
      return @count == 0 ? 0 : @total_elapsed_time / @count
    end

    # 1 秒あたりの実行回数.
    # Transaction Per Seconds.
    # 
    # @return [Float] 1 秒あたりの実行回数
    def tps
      actual_elapsed_time = @count == 0 ? 0 : @end_time - @first_time
      return actual_elapsed_time == 0 ? 0 : @count / actual_elapsed_time
    end

    # 各統計情報を文字列にして返す
    # 
    # @return [String]
    def to_s
      return "tx: \"#{@name}\", " +
             "count: #{@count}, " +
             "error: #{@error_count}, " +
             "first: \"#{time_to_str(@first_time)}\", " +
             "end: \"#{time_to_str(@end_time)}\", " +
             "elapsed: #{@elapsed_time}, " +
             "total: #{@total_elapsed_time}, " +
             "avg: #{sprintf("%.3f", avg_elapsed_time)}, " +
             "max: #{@max_elapsed_time}, " +
             "min: #{@min_elapsed_time}, " +
             "tps: #{tps}"
    end

    # {TransactionStatistic} 同士の統計情報を加えて新たな {TransactionStatistic}
    # インスタンスを生成
    # 
    # @return [TransactionStatistic]
    def +(stat)
      if stat.first_time.nil?
        ret = self.dup
        ret.name = @name + stat.name
        return ret
      elsif @first_time.nil?
        ret = stat.dup
        ret.name = @name + stat.name
        return ret
      else
        stat_hash = Hash.new
        stat_hash[:name] = @name + stat.name
        stat_hash[:count] = @count + stat.count
        stat_hash[:first_time] =
          @first_time < stat.first_time ? @first_time : stat.first_time

        receiver = true
        if @end_time && stat.end_time
          if @end_time > stat.end_time
            receiver = true
          else
            receiver = false
          end
        elsif stat.end_time.nil?
          receiver = true
        else
          receiver = false
        end
        if receiver
          stat_hash[:start_time] = @start_time
          stat_hash[:end_time] = @end_time
          stat_hash[:elapsed_time] = @elapsed_time
          stat_hash[:sql_exception] = @sql_exception
        else
          stat_hash[:start_time] = stat.start_time
          stat_hash[:end_time] = stat.end_time
          stat_hash[:elapsed_time] = stat.elapsed_time
          stat_hash[:sql_exception] = stat.sql_exception
        end

        stat_hash[:total_elapsed_time] = @total_elapsed_time + stat.total_elapsed_time

        if @max_elapsed_time && stat.max_elapsed_time
          stat_hash[:max_elapsed_time] =
            @max_elapsed_time > stat.max_elapsed_time ? @max_elapsed_time : stat.max_elapsed_time
          stat_hash[:min_elapsed_time] =
            @min_elapsed_time < stat.min_elapsed_time ? @min_elapsed_time : stat.min_elapsed_time
        else
          stat_hash[:max_elapsed_time] = @max_elapsed_time || stat.max_elapsed_time
          stat_hash[:min_elapsed_time] = @min_elapsed_time || stat.min_elapsed_time
        end

        stat_hash[:error_count] = @error_count + stat.error_count
        return TransactionStatistic.new(nil, stat_hash)
      end
    end

    # 別時刻の時点の {TransactionStatistic} インスタンスの統計情報の差分を表す
    # 新たな {TransactionStatistic} インスタンスを生成
    # 
    # @return [TransactionStatistic]
    def -(stat)
      if @name != stat.name || @first_time != stat.first_time
        raise "different statistic"
      elsif @count < stat.count
        raise "receiver is older"
      else
        stat_hash = Hash.new
        stat_hash[:name] = @name
        stat_hash[:count] = @count - stat.count
        stat_hash[:first_time] = stat.end_time
        stat_hash[:start_time] = @start_time
        stat_hash[:end_time] = @end_time
        stat_hash[:elapsed_time] = @elapsed_time
        stat_hash[:sql_exception] = @sql_exception
        stat_hash[:elapsed_time] = @elapsed_time
        stat_hash[:total_elapsed_time] = @total_elapsed_time - stat.total_elapsed_time
        stat_hash[:max_elapsed_time] = @max_elapsed_time
        stat_hash[:min_elapsed_time] = @min_elapsed_time
        stat_hash[:error_count] = @error_count - stat.error_count
        return TransactionStatistic.new(nil, stat_hash)
      end
    end

    private
    def time_to_str(time)
      return time.strftime("%Y-%m-%d %X.") + sprintf("%03d", (time.usec / 1000)) if time
    end
  end

  # トランザクションを表すクラス
  class Transaction
    extend Forwardable

    def_delegators :@statistic, :name, :count, :first_time, :start_time,
                                :end_time, :elapsed_time, :total_elapsed_time,
                                :max_elapsed_time, :min_elapsed_time,
                                :error_count, :sql_exception,
                                :avg_elapsed_time, :tps, :to_s

    attr_reader :statistic
    alias_method :stat, :statistic

    # トランザクションを生成
    #
    # @param [String] name トランザクション名
    # @yield 実行するトランザクション
    # @return [Transaction]
    def initialize(name = "", &block)
      @statistic = TransactionStatistic.new(name)
      @transaction = block
    end

    # トランザクションを実行
    def execute
      @before_all.call if @before_all && @statistic.count == 0
      @before_each.call if @before_each
      @statistic.start do
        begin
          @transaction.call
        rescue SQLException => e
          @statistic.sql_exception = e.cause
          @whenever_sqlerror.call if @whenever_sqlerror
        end
      end
      @after_each.call if @after_each
    end

    # 初回トランザクション実行前に1度だけ実行される処理を登録
    def before_all(&block)
      @before_all = block
    end

    # トランザクション実行前に毎回実行される処理を登録
    def before_each(&block)
      @before_each = block
    end
    alias_method :before, :before_each

    # トランザクション実行後に毎回実行される処理を登録
    def after_each(&block)
      @after_each = block
    end
    alias_method :after, :after_each

    # トランザクション実行中に SQLException 発生時に実行される処理を登録
    def whenever_sqlerror(&block)
      @whenever_sqlerror = block
    end
  end
end
