# -*- coding: utf-8 -*-
module RTA
  # 実行時間の分布を表すクラス
  class Distribution
    WIDTH = 0.001 # 分布の幅の秒数

    HISTGRAM_BIN_NUM = 100 # ヒストグラムのビン数

    # ビンを表す配列
    # @return [Array<Number>]
    attr_reader :bins

    def initialize(bins = Array.new)
      @bins = bins
    end

    # 指定された実行時間を追加する
    #
    # @param [Float] elapsed_time 実行時間
    def <<(elapsed_time)
      idx = ((elapsed_time * 1000) / (WIDTH * 1000)).to_i
      @bins[idx] = @bins[idx].to_i + 1
    end

    # 指定されたパーセントのパーセンタイルを返す
    #
    # @param [Number] percent パーセント
    # @return [Float]
    def percentile(percent)
      accum = @bins.inject(Array.new) { |result, n| result << result[-1].to_i + n.to_i }
      total = total_count
      return accum.find_index { |x| x * 100 / total >= percent }.to_i * WIDTH
    end

    # {Distribution} 同士の実行時間分布を加えて新たな {Distribution}
    # インスタンスを生成
    #
    # @param [Distribution] distribution 加える実行時間分布
    # @return [Distribution]
    def +(distribution)
      if @bins.size < distribution.bins.size
        @bins[distribution.bins.size - 1] = 0
      end

      new_bins = @bins.zip(distribution.bins).map { |pair| pair[0].to_i + pair[1].to_i }
      return Distribution.new(new_bins)
    end

    # {Distribution} の実行時間分布の差分を表す新たな {Distribution}
    # インスタンスを生成
    #
    # @param [Distribution] distribution 差分をとる実行時間分布
    # @return [Distribution]
    def -(distribution)
      if @bins.size < distribution.bins.size
        @bins[distribution.bins.size - 1] = 0
      end

      new_bins = @bins.zip(distribution.bins).map { |pair| pair[0].to_i - pair[1].to_i }
      return Distribution.new(new_bins)
    end

    # ヒストグラムを表す文字列を返す
    #
    # @return [String]
    def histgram
      hist = Array.new

      [0.001, 0.01, 0.1, 1].each do |sec|
        hist << sprintf("Frequency Distribution (%.3fsec. - %.3fsec.)", sec, sec * HISTGRAM_BIN_NUM)
        hist << "=============================="
        hist << "          0%        10%       20%       30%       40%       50%       6%%       70%       80%       90%       100%"
        hist << "          +---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+"
        (HISTGRAM_BIN_NUM + 1).times do |n|
          bin_str = String.new
          bin_str << sprintf(">=%7.3fs|", n * sec)
          if n != HISTGRAM_BIN_NUM
            bin = histgram_bins(sec)[n].to_i
          else
            if histgram_bins(sec).size > HISTGRAM_BIN_NUM
              bin = histgram_bins(sec)[HISTGRAM_BIN_NUM..-1].inject { |sum, n| sum.to_i + n.to_i }.to_i
            else
              bin = 0
            end
          end
          percent = total_count == 0 ? 0 : bin.to_f * 100 / total_count
          100.times { |i| bin_str << (i < percent ? "*" : " ") }
          bin_str << sprintf(" %5.1f%%(%d/%d)", percent, bin, total_count)
          hist << bin_str
        end
        hist << ""
      end

      return hist.join("\n")
    end

    private
    def total_count
      return @bins.inject { |sum, n| sum.to_i + n.to_i }
    end

    def histgram_bins(width)
      ret = Array.new

      @bins.each_with_index do |n, idx|
        ret[idx / (width / WIDTH)] = ret[idx / (width / WIDTH)].to_i + n.to_i
      end

      return ret
    end
  end
end
