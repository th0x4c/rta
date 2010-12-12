# RTA -- Ruby Transaction Application Tool
module RTA
  VERSION = '0.3.6'   # Version
  ROOT = File.expand_path(File.dirname(__FILE__)) # full path for lib directory
end

require "#{RTA::ROOT}/rta/transaction"
require "#{RTA::ROOT}/rta/log"
require "#{RTA::ROOT}/rta/session"
require "#{RTA::ROOT}/rta/controller"
