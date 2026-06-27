require "json"
require "net/http"
require "uri"
require_relative "version"
require_relative "sse"

module Feat
  # Raised when the stream endpoint answers with a non-200 status. +code+
  # carries the HTTP status so the run loop can tell terminal auth failures
  # (401 invalid/revoked/expired key, 403 origin not allowed) apart from
  # retryable ones (429 rate limit, 5xx).
  class StreamError < StandardError
    attr_reader :code

    def initialize(message, code: nil)
      super(message)
      @code = code
    end
  end

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
          raise StreamError.new("datafile stream failed: HTTP #{code}", code: code) unless code == 200

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
    # A connection must stay open at least this long before we treat it as
    # healthy and reset backoff. The server seeds a `put` on every
    # (re)connect, so "an event arrived" alone does not prove the connection
    # is stable: a server that seeds then immediately drops would otherwise
    # pin us to a fixed ~1s reconnect cadence forever.
    DEFAULT_MIN_UPTIME = 5.0
    # HTTP statuses that will never succeed with this key/origin, so we stop
    # retrying: 401 (invalid/revoked/expired key), 403 (origin not allowed).
    TERMINAL_STREAM_CODES = [401, 403].freeze
    JOIN_TIMEOUT_SECONDS = 5

    def initialize(url:, api_key:, transport:, on_put:, on_error: nil,
                   initial_backoff: DEFAULT_INITIAL_BACKOFF,
                   max_backoff: DEFAULT_MAX_BACKOFF,
                   min_uptime: DEFAULT_MIN_UPTIME,
                   max_event_bytes: SSEParser::MAX_EVENT_BYTES)
      @url             = url.chomp("/")
      @api_key         = api_key
      @transport       = transport
      @on_put          = on_put
      @on_error        = on_error
      @initial_backoff = initial_backoff
      @max_backoff     = max_backoff
      @min_uptime      = min_uptime
      @max_event_bytes = max_event_bytes
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
        started_at = monotonic
        terminal = false
        begin
          stream_once
        rescue StreamError => e
          notify_error(e)
          terminal = terminal_stream_error?(e)
        rescue StandardError => e
          notify_error(e)
        end
        break if @stop || terminal

        # Only reset backoff for a connection that proved itself by staying
        # open past the minimum uptime, not merely because the seeded `put`
        # was received.
        backoff = @initial_backoff if (monotonic - started_at) >= @min_uptime
        # Equal jitter spreads reconnects so a fleet does not stampede a
        # restarting relay. The un-jittered backoff feeds the next doubling.
        interruptible_sleep(apply_jitter(backoff)) { @stop }
        backoff = next_backoff(backoff)
      end
    end

    def terminal_stream_error?(error)
      error.is_a?(StreamError) && TERMINAL_STREAM_CODES.include?(error.code)
    end

    # Equal jitter: sleep half the window plus a random slice of the other
    # half, keeping the delay within [backoff/2, backoff].
    def apply_jitter(backoff)
      half = backoff / 2.0
      half + (rand * half)
    end

    def next_backoff(backoff)
      [backoff * 2, @max_backoff].min
    end

    def stream_once
      uri = URI.parse("#{@url}#{PATH}")
      conn = @transport.connect(uri: uri, headers: stream_headers)
      @mutex.synchronize { @conn = conn }

      parser = SSEParser.new(max_event_bytes: @max_event_bytes)
      conn.each_chunk do |chunk|
        break if @stop

        parser.feed(chunk) { |event| handle_event(event) }
      end
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

    # We deliberately do not send a Last-Event-ID header to resume. The
    # server reseeds the full datafile on every (re)connect and ignores any
    # resume cursor, and store_datafile is version-guarded so a re-pushed
    # snapshot is a no-op. The parsed event id is therefore left unused:
    # tracking it for resume would be dead code.
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
