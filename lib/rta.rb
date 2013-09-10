# RTA -- Ruby Transaction Application Tool
module RTA
  ROOT = File.expand_path(File.dirname(__FILE__)) # full path for lib directory
end

require "#{RTA::ROOT}/rta/version"
require "#{RTA::ROOT}/rta/transaction"
require "#{RTA::ROOT}/rta/log"
require "#{RTA::ROOT}/rta/session"
require "#{RTA::ROOT}/rta/controller"
