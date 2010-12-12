require 'optparse'
require 'drb'
require 'pp'
require 'irb'

module RTA
  # コマンド制御用のモジュール
  module Controller
    # 実行できるコマンド
    COMMANDS = ["start", "standby", "go", "stop", "cli"]

    # バナー出力
    BANNER = "Usage: rtactl [options] <command> [<file>]\n\n" +
             "Example: rtactl -p 9000 -n 5 start example.rb\n" +
             "         rtactl -p 9000 go\n" +
             "         rtactl -p 9000 -s 3,4 standby\n" +
             "         rtactl -p 9000 cli\n" +
             "         rtactl -p 9000 stop\n\n" +
             "Option: "

    # オプション
    OPTIONS = {
      :port => ['-p', '--port=NUMBER', Numeric, 'specify port number'],
      :numses => ['-n', '--number=NUMBER', Numeric, 'specify number of sessions'],
      :sids => ['-s', '--sid=SID_LIST', Array, 'specify session IDs (CSV)'],
      :help => ['-h', 'output help']
    }

    # オプションを表すクラス
    class Option
      # port 番号
      # @return [Number]
      attr_accessor :port

      # セッション ID の配列
      # @return [Array<Number>]
      attr_accessor :sids

      # セッション数
      # @return [Number]
      attr_accessor :numses

      # 実行するコマンド
      # @return [String]
      attr_reader :command

      # ファイル名
      # @return [String]
      attr_reader :filename

      # Option インスタンスを生成
      def initialize
        @numses = 1
      end

      # 引数を解析
      #
      # @param [Array<String>] argv 引数の文字列の配列
      def parse(argv = ARGV)
        argv = argv.dup
        begin
          op = OptionParser.new
          op.banner = BANNER
          op.on(*OPTIONS[:port]) { |arg| @port = arg }
          op.on(*OPTIONS[:numses]) { |arg| @numses = arg }
          op.on(*OPTIONS[:sids]) { |arg| @sids = arg.map { |sid| sid.to_i } }
          op.on(*OPTIONS[:help]) { STDOUT.puts op.help; exit 0 }
          op.parse!(argv)

          raise "Missing command" if argv.size == 0
          raise "Invalid command: #{argv[0]}" unless COMMANDS.find { |com| com == argv[0] }
          raise "Missing port" unless @port
          if argv[0] == "start" && (argv[1].nil? || (! FileTest.exist?(argv[1])))
            raise "Missing file: #{argv[1]}"
          end

          @command = argv[0]
          @filename = argv[1]
        rescue OptionParser::ParseError => err
          STDERR.puts err.message + "\n" + op.help
          exit 1
        rescue
          STDERR.puts $!.to_s + "\n" + op.help
          exit 1
        end
      end
    end

    # コマンド実行するクラス
    class Runner
      # インスタンスを生成し, {#run} を実行するためのヘルパメソッド
      #
      # @param [Array<String>] argv 引数の文字列の配列
      def self.run(*argv)
        new.run(*argv)
      end

      # 与えられた引数に従い, コマンドを実行
      #
      # @param [Array<String>] argv 引数の文字列の配列
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

      # +start+ コマンド実行
      #
      # @param [Option] opt 解析された {Option} インスタンス
      def start(opt)
        $INHERITORS = Array.new
        RTA::Session.class_eval do
          def self.inherited(subclass)
            $INHERITORS << subclass
          end
        end

        Kernel.load opt.filename
        session_class = $INHERITORS[-1]
        session_class ||= RTA::Session

        sm = RTA::SessionManager.new(opt.numses, session_class)
        sm.run
        sm.start_service(opt.port)
      end
    end
  end
end
