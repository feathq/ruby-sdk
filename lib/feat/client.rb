require "json"
require "net/http"
require "uri"

module Feat
  # Polling HTTP client. Uses stdlib only - zero gem dependencies.
  class Client
    DEFAULT_POLL_INTERVAL = 30.0
    MIN_POLL_INTERVAL = 5.0
    MAX_DATAFILE_BYTES = 10 * 1024 * 1024

    def initialize(api_key:, data_plane_url:, poll_interval: DEFAULT_POLL_INTERVAL, http_client: nil)
      raise ArgumentError, "api_key is required" if api_key.nil? || api_key.empty?
      raise ArgumentError, "data_plane_url is required" if data_plane_url.nil? || data_plane_url.empty?

      assert_https_url!(data_plane_url)

      @api_key       = api_key
      @data_plane_url = data_plane_url.chomp("/")
      @poll_interval = [poll_interval.to_f, MIN_POLL_INTERVAL].max
      @http_client   = http_client
      @datafile      = nil
      @etag          = nil
      @mutex         = Mutex.new
      @stop          = false
      @thread        = nil
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
      raise ArgumentError, "data_plane_url must use https:// (http://localhost allowed for tests)"
    rescue URI::InvalidURIError
      raise ArgumentError, "data_plane_url is not a valid URL"
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
      uri = URI.parse("#{@data_plane_url}/sdk/v1/datafile")
      req = Net::HTTP::Get.new(uri)
      req["Authorization"] = "Bearer #{@api_key}"
      @mutex.synchronize { req["If-None-Match"] = @etag if @etag }

      res = (@http_client || Net::HTTP).start(uri.host, uri.port, use_ssl: uri.scheme == "https") do |http|
        http.request(req)
      end

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
  end
end
