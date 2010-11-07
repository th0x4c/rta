module RTA
  VERSION = '0.2.0'
  ROOT = File.expand_path(File.dirname(__FILE__))
end

require "#{RTA::ROOT}/rta/transaction"
require "#{RTA::ROOT}/rta/log"
require "#{RTA::ROOT}/rta/session"
