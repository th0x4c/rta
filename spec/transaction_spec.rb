require File.dirname(__FILE__) + '/spec_helper'
require 'rta/transaction'

describe RTA::Transaction do
  ELAPS = 0.003
  TX_COUNT = 20
  MID = 5

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
    1.upto(TX_COUNT) do |i|
      @tx.execute
      @mid_stat = @tx.stat.dup if i == MID
    end

    @tx0 = RTA::Transaction.new do
    end

    @tx1 = RTA::Transaction.new("tx1") do
      sleep ELAPS
    end
    @tx1.execute
  end

  describe "#name" do
    it "should be the name of transaction" do
      @tx.name.should == "tx_name"
    end

    it "should be \"\" if the transaction is created without name" do
      @tx0.name.should == ""
    end
  end

  describe "#count" do
    it "should be number of executions" do
      @tx.count.should == TX_COUNT
    end

    it "should be 0 if the transaction is not executed" do
      @tx0.count.should == 0
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

    it "should be nil if the transaction is not executed" do
      @tx0.start_time.should be_nil
    end
  end

  describe "#end_time" do
    it "should be the last end timestamp" do
      @tx.end_time.should be_close(@end_time, 0.002)
    end

    it "should be nil if the transaction is not executed" do
      @tx0.end_time.should be_nil
    end
  end

  describe "#elapsed_time" do
    it "should be the last elapsed time" do
      @tx.elapsed_time.should be_close(ELAPS, 0.0002)
    end

    it "should be 0 if the transaction is not executed" do
      @tx0.elapsed_time.should == 0
    end
  end

  describe "#total_elapsed_time" do
    it "should be the total elapsed time" do
      @tx.total_elapsed_time.should be_close(ELAPS * TX_COUNT, 0.0005 * TX_COUNT)
    end

    it "should be close to actual elapsed time" do
      @tx.total_elapsed_time.should be_close(@end_time - @first_time, 0.0005 * TX_COUNT)
    end

    it "should be 0 if the transaction is not executed" do
      @tx0.total_elapsed_time.should == 0
    end
  end

  describe "#error_count" do
    it "should be number of errors" do
      expected = 0
      (0 .. TX_COUNT - 1).each do |i|
        expected += 1 if i % 4 == 1
      end
      @tx.error_count.should == expected
    end

    it "should be 0 if the transaction is not executed" do
      @tx0.error_count.should == 0
    end
  end

  describe "#avg_elapsed_time" do
    it "should be close to actual elapsed time" do
      @tx.avg_elapsed_time.should be_close(ELAPS, 0.0005)
    end

    it "should be 0 if the transaction is not executed" do
      @tx0.avg_elapsed_time.should == 0
    end
  end

  describe "#tps" do
    it "should be transactions per seconds" do
      @tx.tps.should == TX_COUNT / (@tx.end_time - @tx.first_time)
    end

    it "should be 0 if the transaction is not executed" do
      @tx0.tps.should == 0
    end
  end

  describe "#to_s" do
    it "should be a string includes the name of transaction" do
      @tx.to_s.should include("tx: \"tx_name\"")
    end

    it "should be a string includes number of executions" do
      @tx.to_s.should include("count: #{TX_COUNT}")
    end

    it "should be a string includes \"count: 0\" if the transaction is not executed" do
      @tx0.to_s.should include("count: 0")
    end
  end

  describe "#before_all" do
    it "should be called once an only once before all of the executions" do
      @before_all_count.should == 1
    end
  end

  describe "#before_each" do
    it "should be called before each execution" do
      @before_count.should == TX_COUNT
    end
  end

  describe "#after_each" do
    it "should be called after each execution" do
      @after_count.should == TX_COUNT
    end
  end

  describe "#stat.+" do
    it "should be RTA::Statistic which has sum of transactions" do
      stat = @tx.stat + @tx1.stat
      stat.name.should == @tx.name + @tx1.name
      stat.count.should == TX_COUNT + 1

      stat = @tx1.stat + @tx.stat
      stat.name.should == @tx1.name + @tx.name
      stat.count.should == 1 + TX_COUNT

      stat = @tx.stat + @tx0.stat
      stat.name.should == @tx.name + @tx0.name
      stat.count.should == TX_COUNT + 0

      stat = @tx0.stat + @tx.stat
      stat.name.should == @tx0.name + @tx.name
      stat.count.should == 0 + TX_COUNT
    end

    it "should not change receiver's statistics" do
      stat = @tx.stat + @tx1.stat
      @tx.name.should == "tx_name"
      @tx1.name.should == "tx1"
      @tx.count.should == TX_COUNT
      @tx1.count.should == 1
    end
  end

  describe "#stat.-" do
    it "should be RTA::Statistic which has delta of transactions" do
      stat = @tx.stat - @mid_stat
      stat.name.should == @tx.name
      stat.count.should == TX_COUNT - MID
    end

    it "should not change receiver's statistics" do
      stat = @tx.stat - @mid_stat
      @tx.name.should == "tx_name"
      @mid_stat.name.should == "tx_name"
      @tx.count.should == TX_COUNT
      @mid_stat.count.should == MID
    end

    it "should raise exception if receiver and arg are different" do
      lambda { stat = @tx.stat - @tx1.stat }.should raise_error(RuntimeError)
    end

    it "should raise exception if receiver is older than arg" do
      lambda { stat = @mid_stat - @tx.stat }.should raise_error(RuntimeError)
    end
  end
end
