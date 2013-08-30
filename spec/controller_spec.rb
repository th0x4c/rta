require File.dirname(__FILE__) + '/spec_helper'
require 'rta/controller'

EX_PORT = 9000
EX_NUM = 5
EX_COMMAND = "start"
EX_FILE = "example.rb"
EX_ARGV = "-p #{EX_PORT} -n #{EX_NUM} #{EX_COMMAND} #{EX_FILE}".split

module RTA
  class Session
  end
  class SessionManager
  end
end

describe RTA::Controller::Option do
  before(:each) do
    FileTest.stub(:exist?).with(EX_FILE).and_return(true)
    @option = RTA::Controller::Option.new
  end

  describe "#parse" do
    it "should accept -h and show help" do
      STDOUT.should_receive(:puts).with(/#{Regexp.quote(RTA::Controller::BANNER)}/)
      lambda { @option.parse(["-h"]) }.should raise_error(SystemExit)
    end

    it "should show help and exit if command is not specified" do
      STDERR.should_receive(:puts).with(/#{Regexp.quote(RTA::Controller::BANNER)}/)
      lambda { @option.parse(EX_ARGV - ["start", "example.rb"]) }.should raise_error
    end

    it "should show help and exit if command is invalid" do
      STDERR.should_receive(:puts).with(/#{Regexp.quote(RTA::Controller::BANNER)}/)
      lambda { @option.parse(EX_ARGV - ["start", "example.rb"] + ["invalid_command"]) }.should raise_error
    end

    it "should show help and exit if option is invalid" do
      STDERR.should_receive(:puts).with(/#{Regexp.quote(RTA::Controller::BANNER)}/)
      lambda { @option.parse(EX_ARGV + ["--invalid_option"]) }.should raise_error
    end

    it "should show help and exit if file with \"start\" command does not exist" do
      STDERR.should_receive(:puts).with(/#{Regexp.quote(RTA::Controller::BANNER)}/)
      no_exist_file_name = "no_exist_file.rb"
      FileTest.stub(:exist?).with(no_exist_file_name).and_return(false)
      lambda { @option.parse(EX_ARGV - ["example.rb"] + [no_exist_file_name]) }.should raise_error
    end

    it "should accept -p as port number" do
      @option.parse(EX_ARGV)
      @option.port.should == EX_PORT
    end

    it "should accept -n as number of sessions" do
      @option.parse(EX_ARGV)
      @option.numses.should == EX_NUM
    end

    it "should accept -s as session IDs" do
      @option.parse(EX_ARGV + ["-s", "3"])
      @option.sids.should == [3]

      @option.parse(EX_ARGV + ["-s", "3,4"])
      @option.sids.should == [3, 4]
    end
  end
end

describe RTA::Controller::Runner do
  describe "#run" do
    before(:each) do
      @session_manager = double("RTA::SessionManager")
      DRb.stub(:start_service)
      DRbObject.stub(:new_with_uri).and_return(@session_manager)
    end

    it "should run RTA::SessionManager and start DRb service if command is \"start\"" do
      RTA::SessionManager.should_receive(:new).and_return(@session_manager)
      @session_manager.should_receive(:run)
      @session_manager.should_receive(:start_service)
      FileTest.stub(:exist?).with(EX_FILE).and_return(true)
      Kernel.should_receive(:load).with(EX_FILE).and_return(true)
      RTA::Controller::Runner.run(*EX_ARGV)
    end

    it "should stop RTA::SessionManager if command is \"stop\"" do
      @session_manager.should_receive(:stop)
      ex_argv = "-p #{EX_PORT} stop".split
      RTA::Controller::Runner.run(*ex_argv)
    end

    it "should stop specified sessions if command is \"stop\" with \"-s\"" do
      @session_manager.should_receive(:stop).with([3, 4])
      ex_argv = "-p #{EX_PORT} stop -s 3,4".split
      RTA::Controller::Runner.run(*ex_argv)
    end

    it "should go RTA::SessionManager if command is \"go\"" do
      @session_manager.should_receive(:go)
      ex_argv = "-p #{EX_PORT} go".split
      RTA::Controller::Runner.run(*ex_argv)
    end

    it "should go specified sessions if command is \"go\" with \"-s\"" do
      @session_manager.should_receive(:go).with([3, 4])
      ex_argv = "-p #{EX_PORT} go -s 3,4".split
      RTA::Controller::Runner.run(*ex_argv)
    end

    it "should standby RTA::SessionManager if command is \"standby\"" do
      @session_manager.should_receive(:standby)
      ex_argv = "-p #{EX_PORT} standby".split
      RTA::Controller::Runner.run(*ex_argv)
    end

    it "should standby specified sessions if command is \"standby\" with \"-s\"" do
      @session_manager.should_receive(:standby).with([3, 4])
      ex_argv = "-p #{EX_PORT} standby -s 3,4".split
      RTA::Controller::Runner.run(*ex_argv)
    end

    it "should launch console if command is \"console\"" do
      IRB.should_receive(:start)
      ex_argv = "-p #{EX_PORT} console".split
      RTA::Controller::Runner.run(*ex_argv)
    end
  end
end
