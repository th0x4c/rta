module RTA
  # ログを記録するクラス
  class Log
    OFF = 0 # ログ出力を抑制
    FATAL = 1 # 致命的なエラー
    ERROR = 2 # エラー
    WARN = 3 # 警告
    INFO = 4 # 一般的な情報
    DEBUG = 5 # デバック情報
    ALL = 10 # 必ずログ出力する

    @@files = Hash.new
    @@instance_count = Hash.new

    # ログのレベル
    attr_accessor :level

    # Log インスタンスを生成
    #
    # @param [String] filename ログ出力先のファイル名
    # @return [Log]
    def initialize(filename = nil)
      @level = INFO # デフォルトの level
      unless filename
        @file = STDOUT
        
        # @file.flock を呼べるようにする
        # see RTA::Log#puts
        def @file.flock(op)
        end
      else
        filename = File.expand_path(filename)
        @@instance_count[filename] ||= 0
        if @@instance_count[filename] == 0
          @@files[filename] = File.open(filename, "a")
        end
        @@instance_count[filename] += 1
        @file = @@files[filename]
      end
    end

    # ログをクローズする
    def close
      return if @file.equal?(STDOUT)
      @@instance_count[@file.path] -= 1
      @file.close if @@instance_count[@file.path] == 0
    end

    # メッセージをログに記録する
    # 
    # @param [String] msg 出力するメッセージ
    def puts(msg)
      msg = add_string_to_each_line(time_to_str(Time.now) + " ", msg)
      @file.flock(File::LOCK_EX)
      @file.puts(msg)
      @file.flush
      @file.flock(File::LOCK_UN)
    end

    # {FATAL} 情報のメッセージをログに記録する
    # 
    # @param [String] msg 出力するメッセージ
    # @see FATAL
    def fatal(msg)
      return if @level < FATAL
      msg = add_string_to_each_line("[FATAL] ", msg)
      puts(msg)
    end

    # {ERROR} 情報のメッセージをログに記録する
    # 
    # @param [String] msg 出力するメッセージ
    # @see ERROR
    def error(msg)
      return if @level < ERROR
      msg = add_string_to_each_line("[ERROR] ", msg)
      puts(msg)
    end

    # {WARN} 情報のメッセージをログに記録する
    # 
    # @param [String] msg 出力するメッセージ
    # @see WARN
    def warn(msg)
      return if @level < WARN
      msg = add_string_to_each_line("[WARN] ", msg)
      puts(msg)
    end

    # {INFO} 情報のメッセージをログに記録する
    # 
    # @param [String] msg 出力するメッセージ
    # @see INFO
    def info(msg)
      return if @level < INFO
      msg = add_string_to_each_line("[INFO] ", msg)
      puts(msg)
    end

    # {DEBUG} 情報のメッセージをログに記録する
    # 
    # @param [String] msg 出力するメッセージ
    # @see DEBUG
    def debug(msg)
      return if @level < DEBUG
      msg = add_string_to_each_line("[DEBUG] ", msg)
      puts(msg)
    end

    private
    def add_string_to_each_line(str, msg)
      return str if msg == ""
      return msg.map { |line| str + line }.join
    end

    def time_to_str(time)
      return time.strftime("%Y-%m-%d %X.") + sprintf("%03d", (time.usec / 1000)) if time
    end
  end
end
