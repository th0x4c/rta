require 'java'
import java.sql.SQLException

module RTA
  class Transaction
    # Transaction 名
    # @return [String]
    attr_reader :name

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

    # トランザクションを生成
    # @return [Transaction]
    def initialize(name = nil, &block)
      @name = name
      @count = 0
      @elapsed_time = 0 
      @total_elapsed_time = 0
      @error_count = 0
      @transaction = block
    end

    # トランザクションを実行
    def execute
      @sql_exception = nil
      @before_all.call if @before_all && @count == 0
      @before_each.call if @before_each
      @start_time = Time.now
      @first_time ||= @start_time
      begin
        @transaction.call
      rescue SQLException => e
        @sql_exception = e.cause
        @error_count += 1
      end
      @end_time = Time.now
      @count += 1
      @elapsed_time = @end_time - @start_time
      @total_elapsed_time += @elapsed_time
      if @count == 1
        @max_elapsed_time = @elapsed_time
        @min_elapsed_time = @elapsed_time
      else
        @max_elapsed_time = @elapsed_time > @max_elapsed_time ?
                              @elapsed_time : @max_elapsed_time
        @min_elapsed_time = @elapsed_time < @min_elapsed_time ?
                              @elapsed_time : @min_elapsed_time
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
  end
end

