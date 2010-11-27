require 'optparse'
require 'drb'
require 'pp'
require 'irb'

module RTA
  module Controller
    COMMANDS = ["start", "standby", "go", "stop", "cli"]

    BANNER = "Usage: rtactl [options] <command> [<file>]\n\n" +
             "Example: rtactl -p 9000 -n 5 start example.rb\n" +
             "         rtactl -p 9000 go\n" +
             "         rtactl -p 9000 -s 3,4 standby\n" +
             "         rtactl -p 9000 cli\n" +
             "         rtactl -p 9000 stop\n\n" +
             "Option: "

    OPTIONS = {
      :port => ['-p', '--port=NUMBER', Numeric, 'specify port number'],
      :numses => ['-n', '--number=NUMBER', Numeric, 'specify number of sessions'],
      :sids => ['-s', '--sid=SID_LIST', Array, 'specify session IDs (CSV)'],
      :help => ['-h', 'output help']
    }

    class Option
      attr_accessor :port
      attr_accessor :sids
      attr_accessor :numses

      attr_reader :command
      attr_reader :filename

      def initialize
        @numses = 1
      end

      def parse(argv = ARGV)
        begin
          op = OptionParser.new
          op.banner = BANNER
          op.on(*OPTIONS[:port]) { |arg| @port = arg }
          op.on(*OPTIONS[:numses]) { |arg| @numses = arg }
          op.on(*OPTIONS[:sids]) { |arg| @sids = arg.map { |sid| sid.to_i } }
          op.on(*OPTIONS[:help]) { puts op.help; exit 0 }
          op.parse!(argv)

          raise "Missing command" if argv.size == 0
          raise "Invalid command: #{argv[0]}" unless COMMANDS.find { |com| com == argv[0] }
          raise "Missing port" unless @port
          if ARGV[0] == "start" && (argv[1].nil? || (! FileTest.exist?(argv[1])))
            raise "Missing file: #{argv[1]}"
          end

          @command = argv[0]
          @filename = argv[1]
        rescue OptionParser::ParseError => err
          STDERR.puts err.message
          STDERR.puts op.help
          exit 1
        rescue
          STDERR.puts $! if $!.to_s != ""
          STDERR.puts op.help
          exit 1
        end
      end
    end

    class Runner
      def self.run(*argv)
        new.run(*argv)
      end

      def run(*argv)
        opt = RTA::Controller::Option.new
        opt.parse(argv)

        if opt.command == "start"
          start(opt)
        else
          DRb.start_service
          sm = DRbObject.new_with_uri("druby://localhost:#{opt.port}")

          case opt.command
          when "standby"
            sm.standby(opt.sids)
          when "go"
            sm.go(opt.sids)
          when "stop"
            sm.stop(opt.sids)
          when "cli"
            $rta = sm
            IRB.start(__FILE__)
          end
        end
      end

      def start(opt)
        $INHERITORS = Array.new
        RTA::Session.class_eval do
          def self.inherited(subclass)
            $INHERITORS << subclass
          end
        end

        load opt.filename
        session_class = $INHERITORS[-1]
        session_class ||= RTA::Session

        sm = RTA::SessionManager.new(opt.numses, session_class)
        sm.run
        sm.start_service(opt.port)
      end
    end
  end
end
