require "minitest/autorun"
require "feat"
require_relative "support/stream_helpers"

# Integration tests for the client wiring: streaming updates, version
# ordering, polling fallback, header auth, and clean shutdown.
class ClientTest < Minitest::Test
  ON  = { "id" => "on",  "name" => "on",  "value" => true }.freeze
  OFF = { "id" => "off", "name" => "off", "value" => false }.freeze

  # A boolean flag whose fallthrough variation we flip between versions so
  # an adopted datafile is observable through evaluation.
  def datafile_hash(version:, default_id:)
    {
      "schemaVersion" => 1,
      "envId"         => "env-1",
      "envKey"        => "staging",
      "projectId"     => "proj-1",
      "version"       => version,
      "etag"          => "etag-#{version}",
      "generatedAt"   => "2026-06-26T00:00:00Z",
      "flags"         => {
        "checkout" => {
          "id"                             => "flag-1",
          "key"                            => "checkout",
          "valueType"                      => "boolean",
          "salt"                           => "abcdef0123456789",
          "archived"                       => false,
          "isEnabled"                      => true,
          "offVariationId"                 => "off",
          "defaultVariationId"             => default_id,
          "defaultRollout"                 => nil,
          "defaultBucketingContextKindKey" => nil,
          "variations"                     => [ON, OFF],
          "targets"                        => [],
          "rules"                          => [],
        },
      },
      "segments"     => {},
      "contextKinds" => {
        "user" => { "key" => "user", "availableForRules" => true, "availableForExperiments" => true },
      },
    }
  end

  def put_frame(version:, default_id:)
    "event: put\nid: #{version}\ndata: #{JSON.generate(datafile_hash(version: version, default_id: default_id))}\n\n"
  end

  def ctx
    Feat::EvalContext.new(kinds: { "user" => { "key" => "u1" } })
  end

  def http_serving(*datafiles)
    FakeHTTPClient.new(datafiles.map do |df|
      FakeHTTPResponse.new(code: 200, body: JSON.generate(df), headers: { "ETag" => df["etag"] })
    end)
  end

  def test_streaming_put_with_newer_version_updates_evaluation
    # Initial poll loads version 1 (default off => false).
    http = http_serving(datafile_hash(version: 1, default_id: "off"))
    # Stream pushes version 2 (default on => true).
    transport = FakeStreamTransport.new { |_| FakeStreamConnection.new([put_frame(version: 2, default_id: "on")]) }
    client = Feat::Client.new(api_key: "k", url: "https://example.test", http_client: http, stream_transport: transport)
    client.start

    assert_equal false, client.get_boolean_value("checkout", false, ctx)
    became_true = wait_until { client.get_boolean_value("checkout", false, ctx) == true }
    assert became_true, "expected streamed v2 datafile to flip evaluation to true"
  ensure
    client&.close
  end

  def test_streaming_ignores_equal_and_older_versions
    http = http_serving(datafile_hash(version: 5, default_id: "on")) # starts true
    frames = [
      put_frame(version: 5, default_id: "off"), # equal -> ignore
      put_frame(version: 3, default_id: "off"), # older -> ignore
    ].join
    transport = FakeStreamTransport.new { |_| FakeStreamConnection.new([frames]) }
    client = Feat::Client.new(api_key: "k", url: "https://example.test", http_client: http, stream_transport: transport)
    client.start

    sleep 0.1 # let the stream thread process the (ignored) frames
    assert_equal true, client.get_boolean_value("checkout", false, ctx),
                 "equal/older stream frames must not overwrite the newer datafile"
  ensure
    client&.close
  end

  def test_streaming_sends_authorization_header
    http = http_serving(datafile_hash(version: 1, default_id: "off"))
    transport = FakeStreamTransport.new { |_| FakeStreamConnection.new([put_frame(version: 2, default_id: "on")]) }
    client = Feat::Client.new(api_key: "secret-key", url: "https://example.test", http_client: http, stream_transport: transport)
    client.start

    headers = wait_until { transport.headers_seen.first }
    assert_equal "Bearer secret-key", headers["Authorization"]
  ensure
    client&.close
  end

  def test_polling_fallback_version_guard_when_streaming_disabled
    # No stream; three sequential polls: v1, then v3, then a stale v2.
    http = http_serving(
      datafile_hash(version: 1, default_id: "off"),
      datafile_hash(version: 3, default_id: "on"),
      datafile_hash(version: 2, default_id: "off"),
    )
    client = Feat::Client.new(api_key: "k", url: "https://example.test", streaming: false, http_client: http)
    client.start # first poll -> v1
    assert_equal false, client.get_boolean_value("checkout", false, ctx)

    client.refresh # v3
    assert_equal true, client.get_boolean_value("checkout", false, ctx)

    client.refresh # stale v2 -> ignored
    assert_equal true, client.get_boolean_value("checkout", false, ctx),
                 "a stale poll must not clobber the newer datafile"
  ensure
    client&.close
  end

  def test_close_shuts_down_cleanly
    http = http_serving(datafile_hash(version: 1, default_id: "off"))
    transport = FakeStreamTransport.new { |_| FakeStreamConnection.new([put_frame(version: 2, default_id: "on")]) }
    client = Feat::Client.new(api_key: "k", url: "https://example.test", http_client: http, stream_transport: transport)
    client.start
    wait_until { client.get_boolean_value("checkout", false, ctx) == true }

    client.close # should not raise and should stop reconnecting
    before = transport.connect_count
    sleep 0.1
    assert_equal before, transport.connect_count, "no reconnects expected after close"
  end

  # Temporarily lower the poll-interval floor so the background poll thread
  # reaches its first fetch quickly instead of after the 5s production floor.
  def with_min_poll_interval(value)
    original = Feat::Client::MIN_POLL_INTERVAL
    Feat::Client.send(:remove_const, :MIN_POLL_INTERVAL)
    Feat::Client.const_set(:MIN_POLL_INTERVAL, value)
    yield
  ensure
    Feat::Client.send(:remove_const, :MIN_POLL_INTERVAL)
    Feat::Client.const_set(:MIN_POLL_INTERVAL, original)
  end

  def test_close_joins_poll_thread_blocked_in_fetch
    with_min_poll_interval(0.01) do
      body = JSON.generate(datafile_hash(version: 1, default_id: "off"))
      http = BlockingHTTPClient.new(first_body: body, headers: { "ETag" => "e1" })
      client = Feat::Client.new(api_key: "k", url: "https://example.test",
                                streaming: false, poll_interval: 0.01, http_client: http)
      client.start
      http.entered.pop # poll thread is now parked inside fetch_once

      # close() sets @stop then joins the poll thread; the thread is blocked in
      # fetch, so keep releasing it until close returns. With @stop already set,
      # the poll loop exits the next time a fetch unblocks.
      closer = Thread.new { client.close }
      http.release! until closer.join(0.05)

      assert closer.value, "close returned (did not hang on the blocked poll)"
      assert_nil client.instance_variable_get(:@thread), "expected @thread cleared after close"
    end
  end

  def test_evaluate_before_start_returns_default
    client = Feat::Client.new(api_key: "k", url: "https://example.test", streaming: false, http_client: FakeHTTPClient.new([]))
    result = client.evaluate("checkout", "fallback", ctx)
    assert_equal "fallback", result.value
    assert_equal Feat::Reason::ERROR, result.reason
  end
end
