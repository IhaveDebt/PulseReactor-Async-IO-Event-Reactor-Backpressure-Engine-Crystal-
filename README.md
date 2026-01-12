require "socket"
require "time"

module Pulse
  struct Metrics
    property connections : Int32 = 0
    property messages : Int64 = 0
    property start_time : Time = Time.utc

    def report
      elapsed = Time.utc - start_time
      puts "\n=== PulseReactor Metrics ==="
      puts "Connections: #{connections}"
      puts "Messages: #{messages}"
      puts "Uptime: #{elapsed.total_seconds.round(2)}s"
      puts "Throughput: #{messages / elapsed.total_seconds} msg/s"
    end
  end

  class Backpressure
    def initialize(@limit : Int32)
      @queue = Channel(String).new(@limit)
    end

    def push(msg : String)
      @queue.send(msg)
    end

    def pop : String
      @queue.receive
    end
  end

  class Connection
    getter socket : TCPSocket
    getter backpressure : Backpressure

    def initialize(@socket : TCPSocket)
      @backpressure = Backpressure.new(32)
    end

    def handle(metrics : Metrics)
      spawn do
        while line = socket.gets
          metrics.messages += 1
          backpressure.push(line)
        end
      rescue
        socket.close
      end

      spawn do
        loop do
          msg = backpressure.pop
          socket << "ACK: #{msg}"
        end
      end
    end
  end

  class Reactor
    def initialize(@port : Int32)
      @server = TCPServer.new("0.0.0.0", @port)
      @metrics = Metrics.new
      @connections = [] of Connection
    end

    def run
      puts "PulseReactor listening on port #{@port}"

      spawn do
        loop do
          sleep 10
          @metrics.report
        end
      end

      loop do
        socket = @server.accept
        conn = Connection.new(socket)
        @connections << conn
        @metrics.connections += 1
        conn.handle(@metrics)
      end
    end
  end
end

Pulse::Reactor.new(4000).run
