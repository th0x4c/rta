# -*- coding: utf-8 -*-
module RTA
  # スループットなどを監視するクラス
  class Monitor
    INTERVAL = 5 # 監視間隔
    PAGE_SIZE = 24 # ヘッダー以下の行数
    SAVE_INTERVALS = [5, 10, 30, 60, 300, 600, 1800, 3600] # グラフ出力時の間隔(秒)
    SAVE_COUNT = 100 # グラフ出力時の最小行数

    # Monitor クラスを生成
    # return [Monitor]
    def initialize
      @status = :go
      @count = 0
      @save_stats = Hash.new { |hash, key| hash[key] = Array.new }
    end

    # 監視を別スレッドで開始
    #
    # @param [#stat] target 監視対象
    # @yield 監視対象の {TransactionStatistics} を返すブロック
    def start(target, &block)
      @thread = Thread.new do
        run(target, &block)
      end
    end

    # 監視を開始
    #
    # @param [#stat] target 監視対象
    # @yield 監視対象の {TransactionStatistics} を返すブロック
    def run(target, &block)
      pre_stat = nil

      while @status == :go
        sleep INTERVAL unless @count == 0

        stat = block ? block.call(target) : target.stat
        time = Time.now
        diff_stat = (pre_stat && pre_stat.count > 0) ? stat - pre_stat : stat

        out = Array.new
        if (@count % PAGE_SIZE == 0)
          out << ""
          out << "Time           Transaction  Count        Error    Average  TPS"
          out << "-------------- ------------ ------------ -------- -------- ------------"
        end
        out << time.strftime("%m-%d %X") +
               sprintf(" %-12s %12d %8d %8.6f %12.3f", diff_stat.name, diff_stat.count,
                       diff_stat.error_count, diff_stat.avg_elapsed_time, diff_stat.tps)
        puts out.join("\n")

        save_stat(time, stat)

        pre_stat = stat
        @count += 1
      end

      SAVE_INTERVALS.find_all { |si| ((@count - 1) * INTERVAL) % si != 0 }.each do |si|
        @save_stats[si] << [time, stat.dup] if @save_stats[si]
      end
    end

    # 監視を終了
    def stop
      @status = :stop
      @thread.join if @thread
    end

    # 時系列のスループットを表す文字列を返す
    #
    # @return [String]
    def throughput_graph
      tp = Array.new

      interval = SAVE_INTERVALS.find { |si| @save_stats[si] }
      stats_with_time = @save_stats[interval]

      diff_stats = Array.new
      stats_with_time.count.times do |n|
        time = stats_with_time[n][0]
        diff = if n == 0 || stats_with_time[n - 1][1].count == 0
                 stats_with_time[n][1]
               else
                 stats_with_time[n][1] - stats_with_time[n - 1][1]
               end
        diff_stats << [time, diff]
      end

      max_tps = diff_stats.map { |ds| ds[1].tps }.max
      max_count = diff_stats.map { |ds| ds[1].count }.max
      max_graph = max_tps <= 100 ? 100 :
                  (2 + max_tps.to_i / 10 ** (max_tps.to_i.to_s.size - 2)) * (10 ** (max_tps.to_i.to_s.size - 2))

      tp << sprintf("Throughput (%s)", stats_with_time[0][1].name)
      tp << "=============================="
      tp << "              0%        10%       20%       30%       40%       50%       60%       70%       80%       90%       100%(#{max_graph} tps)"
      tp << "              +---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+"

      diff_stats.each do |ds|
        time = ds[0]
        diff = ds[1]

        percent_str = String.new
        percent_str = time.strftime("%m-%d %X") + "|"
        percent = diff.tps * 100 / max_graph
        100.times { |i| percent_str << ((i >= percent - 1 && i < percent) ? "*" : " ") }
        percent_str << sprintf(" %#{max_graph.to_s.size + 3}.2f tps / %#{max_count.to_s.size}d",
                               diff.tps, diff.count)
        tp << percent_str
      end

      return tp.join("\n")
    end

    private
    def save_stat(time, stat)
      dup_stat = stat.dup
      SAVE_INTERVALS.find_all { |si| (@count * INTERVAL) % si == 0 }.each do |si|
        @save_stats[si] << [time, dup_stat] if @save_stats[si]
      end

      SAVE_INTERVALS.zip(SAVE_INTERVALS[1..-1]) do |si, next_si|
        @save_stats[si] = nil if @save_stats[next_si] && @save_stats[next_si].count >= SAVE_COUNT
      end
    end
  end
end
