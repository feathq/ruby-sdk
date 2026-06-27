require "minitest/autorun"
require "feat"
require_relative "support/stream_helpers"

# Integration tests for the client wiring: streaming updates, version
# ordering, polling fallback, header auth, and clean shutdown.
class ClientTest < Minitest::Test
  ON  = { "id" => "on",  "name" => "on",  "value" => true }.freeze
  OFF = { "id" => "off", "name" => "off", "value" => false }.freeze

  # A boolean flag whose fallthrough variation we flip so an adopted datafile
  # (or a merged patch) is observable through evaluation.
  def flag_hash(default_id:)
    {
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
    }
  end

  def datafile_hash(version:, default_id:)
    {
      "schemaVersion" => 1,
      "envId"         => "env-1",
      "envKey"        => "staging",
      "projectId"     => "proj-1",
      "version"       => version,
      "etag"          => "etag-#{version}",
      "generatedAt"   => "2026-06-26T00:00:00Z",
      "flags"         => { "checkout" => flag_hash(default_id: default_id) },
      "segments"     => {},
      "contextKinds" => {
        "user" => { "key" => "user", "availableForRules" => true, "availableForExperiments" => true },
      },
    }
  end

  def put_frame(version:, default_id:)
    "event: put\nid: #{version}\ndata: #{JSON.generate(datafile_hash(version: version, default_id: default_id))}\n\n"
  end

  # A `patch` SSE frame. Defaults to empty deltas so callers only spell out
  # the fields under test.
  def patch_frame(from:, to:, flags: {}, removed_flags: [], segments: {}, removed_segments: [], etag: nil)
    data = {
      "from"           => from,
      "to"             => to,
      "etag"           => etag || "etag-#{to}",
      "generatedAt"    => "2026-06-26T00:00:00Z",
      "flags"          => flags,
      "removedFlags"   => removed_flags,
      "segments"       => segments,
      "removedSegments" => removed_segments,
    }
    "event: patch\nid: #{to}\ndata: #{JSON.generate(data)}\n\n"
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

  def test_streaming_patch_applies_when_version_matches
    # Initial poll loads v1 (default off => false). A patch from:1 to:2 swaps
    # in a flag whose fallthrough is on, so evaluation must flip to true.
    http = http_serving(datafile_hash(version: 1, default_id: "off"))
    patch = patch_frame(from: 1, to: 2, flags: { "checkout" => flag_hash(default_id: "on") })
    transport = FakeStreamTransport.new { |_| FakeStreamConnection.new([patch]) }
    client = Feat::Client.new(api_key: "k", url: "https://example.test", http_client: http, stream_transport: transport)
    client.start

    assert_equal false, client.get_boolean_value("checkout", false, ctx)
    became_true = wait_until { client.get_boolean_value("checkout", false, ctx) == true }
    assert became_true, "expected patch from:1 to:2 to flip evaluation to true"

    df = client.instance_variable_get(:@datafile)
    assert_equal 2, df.version, "patch should advance the in-memory version to :to"
    assert_equal "etag-2", client.instance_variable_get(:@etag), "patch should advance the etag"
  ensure
    client&.close
  end

  def test_streaming_patch_removes_flag
    # Start with the flag present (default on => true); a patch that lists it
    # in removedFlags must drop it, so evaluation falls back to the default.
    http = http_serving(datafile_hash(version: 1, default_id: "on"))
    patch = patch_frame(from: 1, to: 2, removed_flags: ["checkout"])
    transport = FakeStreamTransport.new { |_| FakeStreamConnection.new([patch]) }
    client = Feat::Client.new(api_key: "k", url: "https://example.test", http_client: http, stream_transport: transport)
    client.start

    assert_equal true, client.get_boolean_value("checkout", false, ctx)
    gone = wait_until { client.evaluate("checkout", false, ctx).reason == Feat::Reason::ERROR }
    assert gone, "expected the removed flag to be absent after the patch"
    assert_equal false, client.get_boolean_value("checkout", false, ctx),
                 "a removed flag should evaluate to the caller default"
  ensure
    client&.close
  end

  def test_streaming_patch_ignored_on_version_mismatch
    # In-memory version is 5; a patch whose from is 1 does not line up, so it
    # must be ignored rather than misapplied on top of the wrong base.
    http = http_serving(datafile_hash(version: 5, default_id: "on"))
    patch = patch_frame(from: 1, to: 2, flags: { "checkout" => flag_hash(default_id: "off") })
    transport = FakeStreamTransport.new { |_| FakeStreamConnection.new([patch]) }
    client = Feat::Client.new(api_key: "k", url: "https://example.test", http_client: http, stream_transport: transport)
    client.start

    sleep 0.1 # let the stream thread process the (ignored) patch
    assert_equal true, client.get_boolean_value("checkout", false, ctx),
                 "a gapped patch (from != current version) must not be applied"
    assert_equal 5, client.instance_variable_get(:@datafile).version,
                 "an ignored patch must not advance the version"
  ensure
    client&.close
  end

  def test_streaming_malformed_patch_is_ignored
    # A patch carrying a broken flag object cannot be merged; it must be
    # dropped without killing the stream thread, and a following well-formed
    # patch from the same base must still apply.
    http = http_serving(datafile_hash(version: 1, default_id: "off"))
    bad  = patch_frame(from: 1, to: 2, flags: { "checkout" => { "id" => "broken" } })
    good = patch_frame(from: 1, to: 2, flags: { "checkout" => flag_hash(default_id: "on") })
    transport = FakeStreamTransport.new { |_| FakeStreamConnection.new([bad + good]) }
    client = Feat::Client.new(api_key: "k", url: "https://example.test", http_client: http, stream_transport: transport)
    client.start

    became_true = wait_until { client.get_boolean_value("checkout", false, ctx) == true }
    assert became_true, "a malformed patch must be skipped and the next one still applied"
    assert_equal 2, client.instance_variable_get(:@datafile).version
  ensure
    client&.close
  end

  def test_streaming_chained_patches_apply_in_order
    # Two patches that chain off each other: 1->2 flips on, 2->3 flips back
    # off. Both must apply in sequence, leaving version 3 and the v3 fallthrough.
    http = http_serving(datafile_hash(version: 1, default_id: "off"))
    chain = [
      patch_frame(from: 1, to: 2, flags: { "checkout" => flag_hash(default_id: "on") }),
      patch_frame(from: 2, to: 3, flags: { "checkout" => flag_hash(default_id: "off") }),
    ].join
    transport = FakeStreamTransport.new { |_| FakeStreamConnection.new([chain]) }
    client = Feat::Client.new(api_key: "k", url: "https://example.test", http_client: http, stream_transport: transport)
    client.start

    reached_v3 = wait_until { client.instance_variable_get(:@datafile).version == 3 }
    assert reached_v3, "expected both chained patches to apply, advancing to version 3"
    assert_equal false, client.get_boolean_value("checkout", false, ctx),
                 "final evaluation should reflect the last patch in the chain"
    assert_equal "etag-3", client.instance_variable_get(:@etag)
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
