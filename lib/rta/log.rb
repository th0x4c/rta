module RTA
  class Log
    OFF = 0
    FATAL = 1
    ERROR = 2
    WARN = 3
    INFO = 4
    DEBUG = 5
    ALL = 10

    @@files = Hash.new

    attr_accessor :level

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
        @@files[filename] ||= File.open(filename, "a")
        @file = @@files[filename]
      end
    end

    def puts(msg)
      msg = time_to_str(Time.now) + " " + msg
      @file.flock(File::LOCK_EX)
      @file.puts(msg)
      @file.flush
      @file.flock(File::LOCK_UN)
    end

    def fatal(msg)
      return if @level < FATAL
      msg = "[FATAL] " + msg
      puts(msg)
    end

    def error(msg)
      return if @level < ERROR
      msg = "[ERROR] " + msg
      puts(msg)
    end

    def warn(msg)
      return if @level < WARN
      msg = "[WARN] " + msg
      puts(msg)
    end

    def info(msg)
      return if @level < INFO
      msg = "[INFO] " + msg
      puts(msg)
    end

    def debug(msg)
      return if @level < DEBUG
      msg = "[DEBUG] " + msg
      puts(msg)
    end

    private
    def time_to_str(time)
      return time.strftime("%Y-%m-%d %X.") + sprintf("%03d", (time.usec / 1000))
    end
  end
end
