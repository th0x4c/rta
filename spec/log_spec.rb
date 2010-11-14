ROOT = File.expand_path(File.dirname(__FILE__))
require "#{ROOT}/../lib/rta/log"

describe RTA::Log do
  FILENAME = "./test.log"

  before(:each) do
    File.unlink(FILENAME) if FileTest.exists?(FILENAME)
    @log = RTA::Log.new(FILENAME)
  end

  after(:each) do
    @log.close
    File.unlink(FILENAME) if FileTest.exists?(FILENAME)
  end

  describe "#new" do
    it "should be the same file if RTA::Log is created for the existing RTA::Log file" do
      filename = File.expand_path(FILENAME)
      log = RTA::Log.new(filename)
      @log.puts("This is the existing RTA::Log.")
      log.puts("This is new RTA::Log.")
      File.open(FILENAME) do |f|
        str = f.read
        str.should match(/This is the existing RTA::Log/)
        str.should match(/This is new RTA::Log/)
      end
      log.close
    end

    it "should log to STDOUT if no args" do
      log = RTA::Log.new
      STDOUT.should_receive(:puts).with(/log/)
      log.fatal("log")
      log.close
    end

  end

  describe "#close" do
    it "should be still open if other instance for the same file is closed" do
      filename = File.expand_path(FILENAME)
      log = RTA::Log.new(filename)
      @log.puts("This is the existing RTA::Log.")
      log.puts("This is new RTA::Log.")
      log.close
      @log.puts("This is still open.")
      File.open(FILENAME) do |f|
        str = f.read
        str.should match(/This is the existing RTA::Log/)
        str.should match(/This is new RTA::Log/)
        str.should match(/This is still open/)
      end
    end
  end

  describe "#level, #level=" do
    it "should be RTA::Log::INFO if default" do
      @log.level.should == RTA::Log::INFO
    end

    it "should be modified if specified" do
      @log.level = RTA::Log::DEBUG
      @log.level.should == RTA::Log::DEBUG
    end
  end

  describe "#fatal" do
    it "should not be logged if level == OFF" do
      @log.level = RTA::Log::OFF
      @log.fatal("fatal")
      File.open(FILENAME) do |f|
        str = f.read
        str.should_not match(/\[FATAL\] fatal/)
      end
    end

    it "should be logged if level == FATAL" do
      @log.level = RTA::Log::FATAL
      @log.fatal("fatal")
      File.open(FILENAME) do |f|
        str = f.read
        str.should match(/\[FATAL\] fatal/)
      end
    end

    it "should be logged if level == ERROR" do
      @log.level = RTA::Log::ERROR
      @log.fatal("fatal")
      File.open(FILENAME) do |f|
        str = f.read
        str.should match(/\[FATAL\] fatal/)
      end
    end

    it "should be logged if level == WARN" do
      @log.level = RTA::Log::WARN
      @log.fatal("fatal")
      File.open(FILENAME) do |f|
        str = f.read
        str.should match(/\[FATAL\] fatal/)
      end
    end

    it "should be logged if level == DEBUG" do
      @log.level = RTA::Log::INFO
      @log.fatal("fatal")
      File.open(FILENAME) do |f|
        str = f.read
        str.should match(/\[FATAL\] fatal/)
      end
    end

    it "should be logged if level == ALL" do
      @log.level = RTA::Log::ALL
      @log.fatal("fatal")
      File.open(FILENAME) do |f|
        str = f.read
        str.should match(/\[FATAL\] fatal/)
      end
    end
  end

  describe "#error" do
    it "should not be logged if level == OFF" do
      @log.level = RTA::Log::OFF
      @log.error("error")
      File.open(FILENAME) do |f|
        str = f.read
        str.should_not match(/\[ERROR\] error/)
      end
    end

    it "should not be logged if level == FATAL" do
      @log.level = RTA::Log::FATAL
      @log.error("error")
      File.open(FILENAME) do |f|
        str = f.read
        str.should_not match(/\[ERROR\] error/)
      end
    end

    it "should be logged if level == ERROR" do
      @log.level = RTA::Log::ERROR
      @log.error("error")
      File.open(FILENAME) do |f|
        str = f.read
        str.should match(/\[ERROR\] error/)
      end
    end

    it "should be logged if level == WARN" do
      @log.level = RTA::Log::WARN
      @log.error("error")
      File.open(FILENAME) do |f|
        str = f.read
        str.should match(/\[ERROR\] error/)
      end
    end

    it "should be logged if level == INFO" do
      @log.level = RTA::Log::INFO
      @log.error("error")
      File.open(FILENAME) do |f|
        str = f.read
        str.should match(/\[ERROR\] error/)
      end
    end

    it "should be logged if level == DEBUG" do
      @log.level = RTA::Log::DEBUG
      @log.error("error")
      File.open(FILENAME) do |f|
        str = f.read
        str.should match(/\[ERROR\] error/)
      end
    end

    it "should be logged if level == ALL" do
      @log.level = RTA::Log::ALL
      @log.error("error")
      File.open(FILENAME) do |f|
        str = f.read
        str.should match(/\[ERROR\] error/)
      end
    end
  end

  describe "#warn" do
    it "should not be logged if level == OFF" do
      @log.level = RTA::Log::OFF
      @log.warn("warn")
      File.open(FILENAME) do |f|
        str = f.read
        str.should_not match(/\[WARN\] warn/)
      end
    end

    it "should not be logged if level == FATAL" do
      @log.level = RTA::Log::FATAL
      @log.warn("warn")
      File.open(FILENAME) do |f|
        str = f.read
        str.should_not match(/\[WARN\] warn/)
      end
    end

    it "should not be logged if level == ERROR" do
      @log.level = RTA::Log::ERROR
      @log.warn("warn")
      File.open(FILENAME) do |f|
        str = f.read
        str.should_not match(/\[WARN\] warn/)
      end
    end

    it "should be logged if level == WARN" do
      @log.level = RTA::Log::WARN
      @log.warn("warn")
      File.open(FILENAME) do |f|
        str = f.read
        str.should match(/\[WARN\] warn/)
      end
    end

    it "should be logged if level == INFO" do
      @log.level = RTA::Log::INFO
      @log.warn("warn")
      File.open(FILENAME) do |f|
        str = f.read
        str.should match(/\[WARN\] warn/)
      end
    end

    it "should be logged if level == DEBUG" do
      @log.level = RTA::Log::DEBUG
      @log.warn("warn")
      File.open(FILENAME) do |f|
        str = f.read
        str.should match(/\[WARN\] warn/)
      end
    end

    it "should be logged if level == ALL" do
      @log.level = RTA::Log::ALL
      @log.warn("warn")
      File.open(FILENAME) do |f|
        str = f.read
        str.should match(/\[WARN\] warn/)
      end
    end
  end

  describe "#info" do
    it "should not be logged if level == OFF" do
      @log.level = RTA::Log::OFF
      @log.info("info")
      File.open(FILENAME) do |f|
        str = f.read
        str.should_not match(/\[INFO\] info/)
      end
    end

    it "should not be logged if level == FATAL" do
      @log.level = RTA::Log::FATAL
      @log.info("info")
      File.open(FILENAME) do |f|
        str = f.read
        str.should_not match(/\[INFO\] info/)
      end
    end

    it "should not be logged if level == ERROR" do
      @log.level = RTA::Log::ERROR
      @log.info("info")
      File.open(FILENAME) do |f|
        str = f.read
        str.should_not match(/\[INFO\] info/)
      end
    end

    it "should not be logged if level == WARN" do
      @log.level = RTA::Log::WARN
      @log.info("info")
      File.open(FILENAME) do |f|
        str = f.read
        str.should_not match(/\[INFO\] info/)
      end
    end

    it "should be logged if level == INFO" do
      @log.level = RTA::Log::INFO
      @log.info("info")
      File.open(FILENAME) do |f|
        str = f.read
        str.should match(/\[INFO\] info/)
      end
    end

    it "should be logged if level == DEBUG" do
      @log.level = RTA::Log::DEBUG
      @log.info("info")
      File.open(FILENAME) do |f|
        str = f.read
        str.should match(/\[INFO\] info/)
      end
    end

    it "should be logged if level == ALL" do
      @log.level = RTA::Log::ALL
      @log.info("info")
      File.open(FILENAME) do |f|
        str = f.read
        str.should match(/\[INFO\] info/)
      end
    end
  end

  describe "#debug" do
    it "should not be logged if level == OFF" do
      @log.level = RTA::Log::OFF
      @log.debug("debug")
      File.open(FILENAME) do |f|
        str = f.read
        str.should_not match(/\[DEBUG\] debug/)
      end
    end

    it "should not be logged if level == FATAL" do
      @log.level = RTA::Log::FATAL
      @log.debug("debug")
      File.open(FILENAME) do |f|
        str = f.read
        str.should_not match(/\[DEBUG\] debug/)
      end
    end

    it "should not be logged if level == ERROR" do
      @log.level = RTA::Log::ERROR
      @log.debug("debug")
      File.open(FILENAME) do |f|
        str = f.read
        str.should_not match(/\[DEBUG\] debug/)
      end
    end

    it "should not be logged if level == WARN" do
      @log.level = RTA::Log::WARN
      @log.debug("debug")
      File.open(FILENAME) do |f|
        str = f.read
        str.should_not match(/\[DEBUG\] debug/)
      end
    end

    it "should not be logged if level == INFO" do
      @log.level = RTA::Log::INFO
      @log.debug("debug")
      File.open(FILENAME) do |f|
        str = f.read
        str.should_not match(/\[DEBUG\] debug/)
      end
    end

    it "should be logged if level == DEBUG" do
      @log.level = RTA::Log::DEBUG
      @log.debug("debug")
      File.open(FILENAME) do |f|
        str = f.read
        str.should match(/\[DEBUG\] debug/)
      end
    end

    it "should be logged if level == ALL" do
      @log.level = RTA::Log::ALL
      @log.debug("debug")
      File.open(FILENAME) do |f|
        str = f.read
        str.should match(/\[DEBUG\] debug/)
      end
    end
  end
end
