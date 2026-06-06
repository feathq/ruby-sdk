require "json"
require "net/http"
require "socket"
require "uri"
require_relative "version"

module Feat
  # Polling HTTP client. Uses stdlib only - zero gem dependencies.
  class Client
    DEFAULT_URL = "https://data-01.feat.so"
    DEFAULT_POLL_INTERVAL = 30.0
    MIN_POLL_INTERVAL = 5.0
    MAX_DATAFILE_BYTES = 10 * 1024 * 1024
    OPEN_TIMEOUT_SECONDS = 3
    READ_TIMEOUT_SECONDS = 10
    RETRYABLE_CONNECT_ERRORS = [
      Net::OpenTimeout,
      Errno::ETIMEDOUT,
      Errno::ECONNREFUSED,
      Errno::EHOSTUNREACH,
      Errno::ENETUNREACH,
    ].freeze

    def initialize(api_key:, url: DEFAULT_URL, poll_interval: DEFAULT_POLL_INTERVAL, http_client: nil)
      raise ArgumentError, "api_key is required" if api_key.nil? || api_key.empty?

      assert_https_url!(url)

      @api_key       = api_key
      @url           = url.chomp("/")
      @poll_interval = [poll_interval.to_f, MIN_POLL_INTERVAL].max
      @http_client   = http_client
      @datafile      = nil
      @etag          = nil
      @mutex         = Mutex.new
      @stop          = false
      @thread        = nil
      @sticky_ip     = nil
    end

    # Blocking initial fetch; spawns a background poller thread.
    def start
      refresh
      @thread ||= Thread.new { poll_loop }
    end

    def close
      @stop = true
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

    def poll_loop
      until @stop
        sleep @poll_interval
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
        data = JSON.parse(body)
        new_etag = res["ETag"]
        @mutex.synchronize do
          @datafile = Datafile.from_json(data)
          @etag = new_etag if new_etag
        end
        true
      else
        raise "feat: fetch datafile failed: #{res.code}"
      end
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
