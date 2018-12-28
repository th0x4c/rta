require File.dirname(__FILE__) + '/spec_helper'
require 'rta/session'

module RTA
  class Log
  end
  class Transaction
  end
end

class ExampleSession < RTA::Session
  @@setup_first_called = 0
  @@teardown_last_called = 0

  def setup_first
    @@setup_first_called += 1
  end
  def setup
  end
  def transaction
    return RTA::Transaction.new
  end
  def teardown
  end
  def teardown_last
    @@teardown_last_called += 1
  end

  def setup_first_called
    return @@setup_first_called
  end
  def teardown_last_called
    return @@teardown_last_called
  end
end

describe RTA::Session do
  before(:each) do
    @log_stub = double("RTA::Log")
    @log_stub.stub(:info).and_return("RTA::Log#info called")
    @log_stub.stub(:error).and_return("RTA::Log#error called")
    @log_stub.stub(:debug).and_return("RTA::Log#debug called")
    RTA::Log.stub(:new).and_return(@log_stub)

    @tx_stub = double("RTA::Transaction")
    @tx_stub.stub(:execute).and_return(true)
    @tx_stub.stub(:sql_exception).and_return(nil)
    RTA::Transaction.stub(:new).and_return(@tx_stub)

    @session = ExampleSession.new(1)
  end

  describe "#new" do
    it "should have a session_id 1 if 1st arg is 1" do
      @session.session_id.should == 1
    end

    it "should have a default status STANDBY" do
      @session.status.should == RTA::Session::STANDBY
    end
  end

  describe "#log" do
    it "should return RTA::Log instance" do
      @session.log.should == @log_stub
    end
  end

  describe "#standby" do
    it "should change status to RTA::Session::STANDBY" do
      @session.standby
      @session.status.should == RTA::Session::STANDBY
    end
  end

  describe "#go" do
    it "should change status to RTA::Session::GO" do
      @session.go
      @session.status.should == RTA::Session::GO
    end
  end

  describe "#stop" do
    it "should change status to RTA::Session::STOP" do
      @session.stop
      @session.status.should == RTA::Session::STOP
    end
  end

  describe "#run" do
    it "should be running if the status is STANDBY" do
      @session.standby
      thread = Thread.new do
        @session.run
      end
      sleep 1
      @session.status.should == RTA::Session::STANDBY
      thread.alive?.should be true
      thread.kill if thread.alive?
    end

    it "should go if the status gets GO" do
      @session.go
      thread = Thread.new do
        @session.run
      end
      sleep 1
      @session.status.should == RTA::Session::GO
      thread.alive?.should be true
      thread.kill if thread.alive?
    end

    it "should stop if the status gets STOP" do
      @session.standby
      thread = Thread.new do
        @session.run
      end
      sleep 1
      @session.stop
      sleep 0.5
      @session.status.should == RTA::Session::STOP
      thread.alive?.should be false
      thread.kill if thread.alive?
    end

    it "should output error message if SQLException happens" do
      @sql_exception_stub = double("SQLException")
      @sql_exception_stub.stub(:getErrorCode).and_return(600)
      @sql_exception_stub.stub(:getMessage).and_return("Some message")
      @tx_stub.stub(:sql_exception).and_return(@sql_exception_stub)
      @tx_stub.stub(:name).and_return("tx_name")

      @log_stub.should_receive(:error)
      thread = Thread.new do
        @session.run
      end
      @session.go
      sleep 1
      @session.stop
      thread.kill if thread.alive?
    end

    it "should call #setup_first once" do
      session1 = ExampleSession.new(1)
      session2 = ExampleSession.new(2)

      thread1 = Thread.new do
        session1.run
      end
      thread2 = Thread.new do
        session2.run
      end
      sleep 1
      session1.stop
      session2.stop
      sleep 1

      session1.setup_first_called.should == 1
      session2.setup_first_called.should == 1

      thread1.kill if thread1.alive?
      thread2.kill if thread2.alive?
    end
  end
end

describe RTA::SessionManager do
  SESSION_COUNT = 5

  before(:each) do
    @ex_session_mock = double("ExampleSession")
    ExampleSession.stub(:new).and_return(@ex_session_mock)

    @session_manager = RTA::SessionManager.new(SESSION_COUNT, ExampleSession)
  end

  describe "#new" do
    it "should generate instances whose number is arg1 and whose class is arg2" do
      session_count = 0
      @session_manager.each do |ses|
        session_count +=1
        ses.should == @ex_session_mock
      end
      session_count.should == SESSION_COUNT
    end
  end

  describe "#run" do
    it "should make sessions run" do
      @ex_session_mock.should_receive(:run).exactly(SESSION_COUNT).times
      @session_manager.run
    end
  end

  describe "#standby" do
    it "should make all sessions standby if no arg" do
      @ex_session_mock.should_receive(:standby).exactly(SESSION_COUNT).times
      @session_manager.standby
    end

    it "should make the session standby if sid is specified" do
      @ex_session_mock.should_receive(:standby).exactly(1).times
      @session_manager.standby(3)
    end

    it "should make the sessions standby if sids are specified as array" do
      @ex_session_mock.should_receive(:standby).exactly(2).times
      @session_manager.standby([2, 3])
    end
  end

  describe "#go" do
    it "should make all sessions go if no arg" do
      @ex_session_mock.should_receive(:go).exactly(SESSION_COUNT).times
      @session_manager.go
    end

    it "should make the session go if sid is specified" do
      @ex_session_mock.should_receive(:go).exactly(1).times
      @session_manager.go(3)
    end

    it "should make the sessions go if sids are specified as array" do
      @ex_session_mock.should_receive(:go).exactly(2).times
      @session_manager.go([2, 3])
    end
  end

  describe "#stop" do
    it "should make all sessions stop if no arg" do
      @ex_session_mock.should_receive(:stop).exactly(SESSION_COUNT).times
      @session_manager.stop
    end

    it "should make the session stop if sid is specified" do
      @ex_session_mock.should_receive(:stop).exactly(1).times
      @session_manager.stop(3)
    end

    it "should make the sessions stop if sids are specified as array" do
      @ex_session_mock.should_receive(:stop).exactly(2).times
      @session_manager.stop([2, 3])
    end
  end

  describe "#start_service" do
    it "should call DRb.start_service" do
      DRb.should_receive(:start_service)
      @session_manager.start_service(9000)
    end
  end

  describe "#stop_service" do
    it "should call DRb.stop_service" do
      DRb.should_receive(:stop_service)
      @session_manager.stop_service
    end
  end

  describe "#stop_session_count" do
    it "should return a number of stop sessions" do
      @ex_session_mock.stub(:run)
      @ex_session_mock.stub(:status).and_return(RTA::Session::STOP, RTA::Session::GO)
      @session_manager.run
      @session_manager.stop_session_count.should == 1
    end
  end

  describe "#transactions" do
    it "should call RTA::Session#transactions" do
      @ex_session_mock.should_receive(:transactions).exactly(SESSION_COUNT).times.and_return(["tx"])
      @session_manager.transactions.should == ["tx"] * SESSION_COUNT
    end
  end
end
