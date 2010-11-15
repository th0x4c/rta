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
    @@instance_count = Hash.new

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
        @@instance_count[filename] ||= 0
        if @@instance_count[filename] == 0
          @@files[filename] = File.open(filename, "a")
        end
        @@instance_count[filename] += 1
        @file = @@files[filename]
      end
    end

    def close
      return if @file.equal?(STDOUT)
      @@instance_count[@file.path] -= 1
      @file.close if @@instance_count[@file.path] == 0
    end

    def puts(msg)
      msg = add_string_to_each_line(time_to_str(Time.now) + " ", msg)
      @file.flock(File::LOCK_EX)
      @file.puts(msg)
      @file.flush
      @file.flock(File::LOCK_UN)
    end

    def fatal(msg)
      return if @level < FATAL
      msg = add_string_to_each_line("[FATAL] ", msg)
      puts(msg)
    end

    def error(msg)
      return if @level < ERROR
      msg = add_string_to_each_line("[ERROR] ", msg)
      puts(msg)
    end

    def warn(msg)
      return if @level < WARN
      msg = add_string_to_each_line("[WARN] ", msg)
      puts(msg)
    end

    def info(msg)
      return if @level < INFO
      msg = add_string_to_each_line("[INFO] ", msg)
      puts(msg)
    end

    def debug(msg)
      return if @level < DEBUG
      msg = add_string_to_each_line("[DEBUG] ", msg)
      puts(msg)
    end

    private
    def add_string_to_each_line(str, msg)
      return msg.map { |line| str + line }.join
    end

    def time_to_str(time)
      return time.strftime("%Y-%m-%d %X.") + sprintf("%03d", (time.usec / 1000)) if time
    end
  end
end
