ROOT = File.expand_path(File.dirname(__FILE__))
require "#{ROOT}/../lib/rta/transaction"

describe RTA::Transaction do
  ELAPS = 0.03

  before(:all) do
    @tx = RTA::Transaction.new("tx_name") do
      sleep ELAPS
      raise SQLException.new if @count % 4 == 1
    end
    @tx.before_all do
      @count ||= 0
      @before_all_count ||= 0
      @before_count ||= 0
      @after_count ||= 0

      @before_all_count += 1
      @first_time = Time.now
    end
    @tx.before_each do
      @before_count += 1
      @start_time = Time.now
    end
    @tx.after_each do
      @end_time = Time.now
      @after_count += 1
      @count += 1
    end
    10.times do
      @tx.execute
    end
  end

  describe "#name" do
    it "should be the name of transaction" do
      @tx.name.should == "tx_name"
    end
  end

  describe "#count" do
    it "should be number of executions" do
      @tx.count.should == 10
    end
  end

  describe "#first_time" do
    it "should be the first timestamp" do
      @tx.first_time.should be_close(@first_time, 0.002)
    end
  end

  describe "#start_time" do
    it "should be the last start timestamp" do
      @tx.start_time.should be_close(@start_time, 0.002)
    end
  end

  describe "#end_time" do
    it "should be the last end timestamp" do
      @tx.end_time.should be_close(@end_time, 0.002)
    end
  end

  describe "#elapsed_time" do
    it "should be the last elapsed time" do
      @tx.elapsed_time.should be_close(ELAPS, 0.002)
    end
  end

  describe "#total_elapsed_time" do
    it "should be the total elapsed time" do
      @tx.total_elapsed_time.should be_close(ELAPS * 10, 0.01)
    end

    it "should be close to actual elapsed time" do
      @tx.total_elapsed_time.should be_close(@end_time - @first_time, 0.01)
    end
  end

  describe "#error_count" do
    it "should be number of errors" do
      @tx.error_count.should == 3
    end
  end

  describe "#before_all" do
    it "should be called once an only once before all of the executions" do
      @before_all_count.should == 1
    end
  end

  describe "#before_each" do
    it "should be called before each execution" do
      @before_count.should == 10
    end
  end

  describe "#after_each" do
    it "should be called after each execution" do
      @after_count.should == 10
    end
  end
end
