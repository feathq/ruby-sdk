require "json"
require "net/http"
require "uri"
require_relative "version"
require_relative "sse"

module Feat
  # Raised when the stream endpoint answers with a non-200 status.
  class StreamError < StandardError; end

  # Sleeps that wake promptly when a stop flag flips, so background threads
  # shut down without waiting out a full interval.
  module InterruptibleSleep
    SLEEP_GRANULARITY = 0.1

    private

    def interruptible_sleep(seconds, &stop)
      deadline = monotonic + seconds
      loop do
        return if stop.call

        remaining = deadline - monotonic
        return if remaining <= 0

        sleep([SLEEP_GRANULARITY, remaining].min)
      end
    end

    def monotonic
      Process.clock_gettime(Process::CLOCK_MONOTONIC)
    end
  end

  # Default SSE transport built on Net::HTTP streaming reads. Injectable so
  # tests can supply a fake that yields canned chunks.
  class NetHTTPStreamTransport
    def initialize(open_timeout:, read_timeout:)
      @open_timeout = open_timeout
      @read_timeout = read_timeout
    end

    # Open a streaming GET and return a connection handle.
    def connect(uri:, headers:)
      http = Net::HTTP.new(uri.host, uri.port)
      http.use_ssl = (uri.scheme == "https")
      http.open_timeout = @open_timeout
      http.read_timeout = @read_timeout
      http.start
      Connection.new(http, uri, headers)
    end

    # Wraps a started Net::HTTP connection. #each_chunk blocks while the
    # server holds the stream open; #close aborts it from another thread.
    class Connection
      def initialize(http, uri, headers)
        @http = http
        @request = Net::HTTP::Get.new(uri)
        headers.each { |name, value| @request[name] = value }
      end

      def each_chunk
        @http.request(@request) do |response|
          code = response.code.to_i
          raise StreamError, "datafile stream failed: HTTP #{code}" unless code == 200

          response.read_body { |chunk| yield chunk }
        end
      end

      def close
        @http.finish if @http.started?
      rescue StandardError
        nil
      end
    end
  end

  # Holds a long-lived SSE connection to the datafile stream endpoint and
  # invokes +on_put+ with the parsed datafile for every `put` frame. Runs on
  # its own thread, reconnects with exponential backoff, and stops cleanly.
  class StreamingClient
    include InterruptibleSleep

    PATH = "/sdk/v1/datafile/stream".freeze
    DEFAULT_INITIAL_BACKOFF = 1.0
    DEFAULT_MAX_BACKOFF = 60.0
    JOIN_TIMEOUT_SECONDS = 5

    def initialize(url:, api_key:, transport:, on_put:, on_error: nil,
                   initial_backoff: DEFAULT_INITIAL_BACKOFF,
                   max_backoff: DEFAULT_MAX_BACKOFF)
      @url             = url.chomp("/")
      @api_key         = api_key
      @transport       = transport
      @on_put          = on_put
      @on_error        = on_error
      @initial_backoff = initial_backoff
      @max_backoff     = max_backoff
      @mutex           = Mutex.new
      @conn            = nil
      @thread          = nil
      @stop            = false
    end

    def start
      @thread ||= Thread.new { run_loop }
      self
    end

    # Signal shutdown, abort any in-flight read, and join the thread.
    def stop
      @stop = true
      current = @mutex.synchronize { @conn }
      begin
        current&.close
      rescue StandardError
        nil
      end
      @thread&.join(JOIN_TIMEOUT_SECONDS)
      @thread = nil
      self
    end

    private

    def run_loop
      backoff = @initial_backoff
      until @stop
        received = false
        begin
          received = stream_once
        rescue StandardError => e
          notify_error(e)
        end
        break if @stop

        backoff = @initial_backoff if received
        interruptible_sleep(backoff) { @stop }
        backoff = [backoff * 2, @max_backoff].min
      end
    end

    # Returns true if any event was dispatched during the connection.
    def stream_once
      uri = URI.parse("#{@url}#{PATH}")
      conn = @transport.connect(uri: uri, headers: stream_headers)
      @mutex.synchronize { @conn = conn }

      parser = SSEParser.new
      received = false
      conn.each_chunk do |chunk|
        break if @stop

        parser.feed(chunk) do |event|
          received = true
          handle_event(event)
        end
      end
      received
    ensure
      closing = @mutex.synchronize do
        held = @conn
        @conn = nil
        held
      end
      begin
        closing&.close
      rescue StandardError
        nil
      end
    end

    def handle_event(event)
      return unless event[:event] == "put"

      raw = event[:data]
      return if raw.nil? || raw.empty?

      parsed =
        begin
          JSON.parse(raw)
        rescue JSON::ParserError
          return
        end

      @on_put.call(parsed)
    rescue StandardError => e
      notify_error(e)
    end

    def stream_headers
      {
        "Authorization" => "Bearer #{@api_key}",
        "User-Agent"    => "feat-sdk-ruby/#{Feat::VERSION}",
        "Accept"        => "text/event-stream",
        "Cache-Control" => "no-cache",
      }
    end

    def notify_error(error)
      @on_error&.call(error)
    rescue StandardError
      nil
    end
  end
end
