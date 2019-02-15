require 'yaml'
require 'drb'

require File.dirname(__FILE__) + '/script/helper'

class SessionManagerSession < RTA::Session
  include TPCCHelper

  @@mutex = Mutex.new
  @@time_str = Time.now.strftime("%Y%m%d%H%M%S")
  @@monitor = RTA::Monitor.new

  attr_reader :session_manager

  def initialize(sid, log = RTA::Log.new)
    super

    self.log = RTA::Log.new(TPCC_HOME + "/log/tpcc_#{@@time_str}.log")
    config = Hash.new
    File.open(TPCC_HOME + '/config/config.yml', 'r') do |file|
      config = YAML.load(file.read)
    end

    @@mutex.synchronize do
      unless config["remote"][self.session_id - 1].nil?
        @session_manager = DRbObject.new_with_uri("druby://#{config["remote"][self.session_id - 1]}")
        @session_manager.increment_drb_client
      end
    end

    @tx = RTA::Transaction.new("dummy") do
      sleep 0.01
    end
  end

  def period_target(start_time, rampup_interval, measurement_interval,
                    rampdown_interval)
    super

    unless @session_manager.nil?
      @session_manager.period_target(start_time, rampup_interval, measurement_interval,
                                     rampdown_interval)
      @session_manager.go
    end
  end

  def transaction
    return @tx
  end

  def transactions
    return [@tx] if caller(1, 1).first =~ /`check_period'/

    unless @session_manager.nil?
      @@mutex.synchronize do
        return @session_manager.transactions
      end
    else
      return []
    end
  end

  def setup_last
    @@monitor.start(self) do |ses|
      stat = ses.stat_by_name("New-Order", sessions, :tx, :rampup) +
             ses.stat_by_name("New-Order", sessions, :tx, :measurement) +
             ses.stat_by_name("New-Order", sessions, :tx, :rampdown)
      stat.name = "New-Order"
      stat
    end
  end

  def teardown_last
    @@monitor.stop
    log.info("")
    log.info(@@monitor.throughput_graph)

    log.info("")
    log.info(histgram("New-Order"))

    tx_names = ["New-Order", "Payment", "Order-Status", "Delivery",
                "Stock-Level"]
    log.info("")
    log.info(numerical_quantities_summary(tx_names))
    puts ""
    puts numerical_quantities_summary(tx_names)

    sessions.each do |ses|
      ses.session_manager.decrement_drb_client unless ses.session_manager.nil?
    end
  end
end
