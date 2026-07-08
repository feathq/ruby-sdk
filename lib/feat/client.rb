require "json"
require "net/http"
require "socket"
require "uri"
require_relative "version"
require_relative "sse"
require_relative "streaming"

module Feat
  # HTTP client. Uses stdlib only - zero gem dependencies.
  #
  # By default the client streams datafile updates over Server-Sent Events
  # and keeps a slow background poll as a safety net. Disable streaming with
  # `streaming: false` to fall back to polling alone.
  class Client
    include InterruptibleSleep

    DEFAULT_URL = "https://data-01.feat.so"
    DEFAULT_POLL_INTERVAL = 30.0
    # When streaming carries updates, the poll is a backstop only and runs
    # far less often.
    DEFAULT_SAFETY_POLL_INTERVAL = 600.0
    MIN_POLL_INTERVAL = 5.0
    MAX_DATAFILE_BYTES = 10 * 1024 * 1024
    OPEN_TIMEOUT_SECONDS = 3
    READ_TIMEOUT_SECONDS = 10
    # Long-lived stream read: heartbeat comments keep it well under this.
    STREAM_READ_TIMEOUT_SECONDS = 90
    # Bound on how long #close waits for the poll thread to unwind, so a
    # blocked fetch cannot make shutdown hang indefinitely.
    POLL_JOIN_TIMEOUT_SECONDS = 5
    RETRYABLE_CONNECT_ERRORS = [
      Net::OpenTimeout,
      Errno::ETIMEDOUT,
      Errno::ECONNREFUSED,
      Errno::EHOSTUNREACH,
      Errno::ENETUNREACH,
    ].freeze

    def initialize(api_key:, url: DEFAULT_URL, poll_interval: DEFAULT_POLL_INTERVAL,
                   streaming: true, safety_poll_interval: DEFAULT_SAFETY_POLL_INTERVAL,
                   http_client: nil, stream_transport: nil)
      raise ArgumentError, "api_key is required" if api_key.nil? || api_key.empty?

      assert_https_url!(url)

      @api_key           = api_key
      @url               = url.chomp("/")
      @streaming_enabled = streaming
      base_interval      = streaming ? safety_poll_interval : poll_interval
      @poll_interval     = [base_interval.to_f, MIN_POLL_INTERVAL].max
      @http_client       = http_client
      @datafile          = nil
      @etag              = nil
      @mutex             = Mutex.new
      @stop              = false
      @thread            = nil
      @sticky_ip         = nil
      @streaming         = build_streaming_client(stream_transport) if @streaming_enabled
    end

    # Blocking initial fetch; spawns the background poller (and stream).
    def start
      refresh
      @streaming&.start
      @thread ||= Thread.new { poll_loop }
      self
    end

    def close
      @stop = true
      @streaming&.stop
      # Join the poll thread (bounded) so a fetch in flight is waited out and
      # @thread is cleared, leaving the client cleanly restartable.
      @thread&.join(POLL_JOIN_TIMEOUT_SECONDS)
      @thread = nil
      self
    end

    def refresh
      fetch_once
    end

    def evaluate(flag_key, default_value, ctx)
      df = @datafile
      if df.nil?
        return EvaluationResult.new(
          value: default_value, reason: Reason::ERROR,
          error_message: "client not ready: call #start before #evaluate"
        )
      end
      Eval.call(flag_key: flag_key, default_value: default_value, ctx: ctx, datafile: df)
    end

    def get_boolean_value(flag_key, default, ctx)
      r = evaluate(flag_key, default, ctx)
      r.value == true || r.value == false ? r.value : default
    end

    def get_string_value(flag_key, default, ctx)
      r = evaluate(flag_key, default, ctx)
      r.value.is_a?(String) ? r.value : default
    end

    def get_number_value(flag_key, default, ctx)
      r = evaluate(flag_key, default, ctx)
      r.value.is_a?(Numeric) && !(r.value == true || r.value == false) ? r.value : default
    end

    def get_object_value(flag_key, default, ctx)
      r = evaluate(flag_key, default, ctx)
      r.value
    end

    private

    def assert_https_url!(url)
      uri = URI.parse(url)
      return if uri.scheme == "https"
      return if uri.scheme == "http" && %w[localhost 127.0.0.1].include?(uri.host)
      raise ArgumentError, "url must use https:// (http://localhost allowed for tests)"
    rescue URI::InvalidURIError
      raise ArgumentError, "url is not a valid URL"
    end

    # Builds the streaming client; its `put` handler runs through the same
    # version-guarded store as polling, so an older frame never wins.
    def build_streaming_client(transport)
      transport ||= NetHTTPStreamTransport.new(
        open_timeout: OPEN_TIMEOUT_SECONDS,
        read_timeout: STREAM_READ_TIMEOUT_SECONDS,
      )
      StreamingClient.new(
        url: @url,
        api_key: @api_key,
        transport: transport,
        on_put: ->(parsed) { store_datafile(parsed, parsed["etag"]) },
        on_patch: ->(parsed) { apply_patch(parsed) },
      )
    end

    # Apply a streamed `patch` delta. Version-gated: the delta is merged only
    # when the in-memory datafile's version equals the patch's +from+, so the
    # result is exactly the +to+ snapshot. On any gap or mismatch the patch is
    # ignored - a reconnect reseeds a full `put` and the safety poll backstops.
    # Runs under the same mutex as store_datafile, so a subsequent evaluation
    # sees the merged delta atomically. Returns true when applied.
    def apply_patch(patch)
      from = patch["from"]
      to   = patch["to"]
      # Reject malformed or out-of-order deltas before touching the datafile:
      # both bounds must be integers and the patch must move strictly forward.
      # Without the `to > from` guard a backward `to` would roll the in-memory
      # version backward and break version ordering.
      return false unless from.is_a?(Integer) && to.is_a?(Integer) && to > from

      @mutex.synchronize do
        current = @datafile
        return false if current.nil?
        return false unless current.version == from

        @datafile = Datafile.merge_patch(current, patch)
        @etag = patch["etag"] if patch["etag"]
      end
      true
    end

    def poll_loop
      until @stop
        interruptible_sleep(@poll_interval) { @stop }
        break if @stop

        begin
          fetch_once
        rescue StandardError
          # Defensive: keep polling on any transient error.
        end
      end
    end

    def fetch_once
      uri = URI.parse("#{@url}/sdk/v1/datafile")
      req = Net::HTTP::Get.new(uri)
      req["Authorization"] = "Bearer #{@api_key}"
      req["User-Agent"] = "feat-sdk-ruby/#{Feat::VERSION}"
      @mutex.synchronize { req["If-None-Match"] = @etag if @etag }

      res = with_http_connection(uri) { |http| http.request(req) }

      case res.code.to_i
      when 304, 404
        false
      when 200
        length = res["Content-Length"]&.to_i
        raise "datafile exceeds maximum allowed size" if length && length > MAX_DATAFILE_BYTES
        body = res.body
        raise "datafile exceeds maximum allowed size" if body.bytesize > MAX_DATAFILE_BYTES
        store_datafile(JSON.parse(body), res["ETag"])
      else
        raise "feat: fetch datafile failed: #{res.code}"
      end
    end

    # Adopt a parsed datafile only if its version is strictly newer than the
    # one in memory. Shared by the poll and stream paths and guarded by the
    # same mutex the evaluator reads through. Returns true when adopted.
    def store_datafile(parsed, etag)
      candidate = Datafile.from_json(parsed)
      @mutex.synchronize do
        current_version = @datafile&.version
        new_version = candidate.version
        return false if new_version && current_version && new_version <= current_version

        @datafile = candidate
        @etag = etag if etag
      end
      true
    end

    # Net::HTTP doesn't iterate getaddrinfo results on connect failure
    # (Ruby 3.3 has no Happy Eyeballs); ipaddr= lets us pin each attempt
    # to a specific IP while keeping the hostname for SNI.
    def with_http_connection(uri, &block)
      if @http_client
        return @http_client.start(
          uri.host, uri.port,
          use_ssl: uri.scheme == "https",
          open_timeout: OPEN_TIMEOUT_SECONDS,
          read_timeout: READ_TIMEOUT_SECONDS,
        ) { |h| block.call(h) }
      end

      sticky = @mutex.synchronize { @sticky_ip }
      if sticky
        begin
          return attempt_request(uri, sticky, &block)
        rescue *RETRYABLE_CONNECT_ERRORS
          @mutex.synchronize { @sticky_ip = nil if @sticky_ip == sticky }
        end
      end

      addresses = resolve_addresses(uri.host)
      if addresses.empty?
        return Net::HTTP.start(
          uri.host, uri.port,
          use_ssl: uri.scheme == "https",
          open_timeout: OPEN_TIMEOUT_SECONDS,
          read_timeout: READ_TIMEOUT_SECONDS,
        ) { |h| block.call(h) }
      end

      last_error = nil
      addresses.each do |ip|
        next if ip == sticky
        begin
          result = attempt_request(uri, ip, &block)
          @mutex.synchronize { @sticky_ip = ip }
          return result
        rescue *RETRYABLE_CONNECT_ERRORS => e
          last_error = e
        end
      end
      raise last_error
    end

    def attempt_request(uri, ip, &block)
      http = Net::HTTP.new(uri.host, uri.port)
      http.ipaddr = ip
      http.use_ssl = (uri.scheme == "https")
      http.open_timeout = OPEN_TIMEOUT_SECONDS
      http.read_timeout = READ_TIMEOUT_SECONDS
      http.start { |h| block.call(h) }
    end

    def resolve_addresses(host)
      Addrinfo.getaddrinfo(host, nil, nil, :STREAM).map(&:ip_address).uniq
    rescue StandardError
      []
    end
  end
end
