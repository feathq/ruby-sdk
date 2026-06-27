require "minitest/autorun"
require "feat"
require_relative "support/stream_helpers"

# Tests for the SSE connection runner in isolation, using a fake transport.
class StreamingTest < Minitest::Test
  def put_frame(version)
    "event: put\nid: #{version}\ndata: {\"version\":#{version}}\n\n"
  end

  def build(transport, puts_seen:, errors: [])
    Feat::StreamingClient.new(
      url: "https://example.test",
      api_key: "test-key",
      transport: transport,
      on_put: ->(parsed) { puts_seen << parsed },
      on_error: ->(err) { errors << err },
      initial_backoff: 0.01,
      max_backoff: 0.05,
    )
  end

  def test_dispatches_put_and_sends_auth_header
    puts_seen = Queue.new
    transport = FakeStreamTransport.new { |_| FakeStreamConnection.new([put_frame(5)]) }
    client = build(transport, puts_seen: puts_seen)
    client.start

    parsed = wait_until { puts_seen.empty? ? nil : puts_seen.pop }
    assert_equal 5, parsed["version"]

    headers = transport.headers_seen.first
    assert_equal "Bearer test-key", headers["Authorization"]
    assert_equal "text/event-stream", headers["Accept"]
    assert_match %r{/sdk/v1/datafile/stream\z}, transport.uris_seen.first.path
  ensure
    client&.stop
  end

  def test_reconnects_after_failure
    puts_seen = Queue.new
    transport = FakeStreamTransport.new do |attempt|
      if attempt == 1
        RaisingStreamConnection.new(Feat::StreamError.new("boom"))
      else
        FakeStreamConnection.new([put_frame(2)])
      end
    end
    client = build(transport, puts_seen: puts_seen)
    client.start

    parsed = wait_until { puts_seen.empty? ? nil : puts_seen.pop }
    assert_equal 2, parsed["version"]
    assert_operator transport.connect_count, :>=, 2
  ensure
    client&.stop
  end

  def test_clean_shutdown_stops_thread_and_connection
    conn = FakeStreamConnection.new([put_frame(1)])
    puts_seen = Queue.new
    transport = FakeStreamTransport.new { |_| conn }
    client = build(transport, puts_seen: puts_seen)
    client.start

    wait_until { puts_seen.empty? ? nil : true }
    client.stop

    assert conn.closed, "expected the live connection to be closed on stop"
    before = transport.connect_count
    sleep 0.1
    assert_equal before, transport.connect_count, "expected no reconnects after stop"
  end

  def idle_transport
    FakeStreamTransport.new { |_| RaisingStreamConnection.new(StandardError.new("idle")) }
  end

  def test_backoff_grows_and_is_capped
    client = build(idle_transport, puts_seen: Queue.new) # initial 0.01, max 0.05
    seen = [0.01]
    5.times { seen << client.send(:next_backoff, seen.last) }
    assert_operator seen[1], :>, seen[0], "backoff should grow after a failure"
    assert seen.each_cons(2).all? { |a, b| b >= a }, "backoff must be non-decreasing"
    assert_operator seen.last, :<=, 0.05, "backoff must not exceed max_backoff"
    assert_equal 0.05, seen.last, "backoff should saturate at max_backoff"
  end

  def test_reconnect_delay_is_jittered
    client = build(idle_transport, puts_seen: Queue.new)
    samples = Array.new(200) { client.send(:apply_jitter, 10.0) }
    assert(samples.all? { |s| s >= 5.0 && s <= 10.0 },
           "equal jitter must stay within [backoff/2, backoff]")
    assert_operator samples.uniq.length, :>, 1, "reconnect delay should be randomized"
    assert_operator samples.min, :<, 6.0, "jitter should spread toward the low end"
    assert_operator samples.max, :>, 9.0, "jitter should spread toward the high end"
  end

  def test_seed_then_drop_does_not_reconnect_at_fixed_cadence
    # A server that seeds a put then immediately drops the connection. With
    # the old "reset whenever an event arrived" logic this reconnected at a
    # fixed ~initial-backoff cadence forever. The min-uptime guard must let
    # backoff grow instead.
    transport = FakeStreamTransport.new { |_| DroppingStreamConnection.new([put_frame(1)]) }
    client = Feat::StreamingClient.new(
      url: "https://example.test", api_key: "k", transport: transport,
      on_put: ->(_) {}, initial_backoff: 0.05, max_backoff: 5.0, min_uptime: 10.0
    )
    client.start
    wait_until(timeout: 3.0) { transport.connect_count >= 5 }
    client.stop

    gaps = transport.connect_times.each_cons(2).map { |a, b| b - a }
    assert_operator gaps.length, :>=, 4, "expected several reconnect attempts"
    assert_operator gaps.last, :>, gaps.first * 1.5,
                    "reconnect gaps should grow (exponential backoff), not stay fixed"
  end

  def test_terminal_401_stops_and_surfaces
    assert_terminal_status(401)
  end

  def test_terminal_403_stops_and_surfaces
    assert_terminal_status(403)
  end

  def assert_terminal_status(code)
    errors = Queue.new
    transport = FakeStreamTransport.new do |_|
      RaisingStreamConnection.new(Feat::StreamError.new("denied", code: code))
    end
    client = build(transport, puts_seen: Queue.new, errors: errors)
    client.start

    err = wait_until { errors.empty? ? nil : errors.pop }
    assert_equal code, err.code, "terminal error should be surfaced via on_error"
    before = transport.connect_count
    sleep 0.1
    assert_equal before, transport.connect_count, "#{code} is terminal; must not reconnect"
  ensure
    client&.stop
  end

  def test_429_backs_off_and_reconnects
    puts_seen = Queue.new
    transport = FakeStreamTransport.new do |attempt|
      if attempt == 1
        RaisingStreamConnection.new(Feat::StreamError.new("rate limited", code: 429))
      else
        FakeStreamConnection.new([put_frame(7)])
      end
    end
    client = build(transport, puts_seen: puts_seen)
    client.start

    parsed = wait_until { puts_seen.empty? ? nil : puts_seen.pop }
    assert_equal 7, parsed["version"], "429 should retry, not terminate"
    assert_operator transport.connect_count, :>=, 2
  ensure
    client&.stop
  end

  def test_oversized_stream_payload_aborts_and_surfaces
    errors = Queue.new
    puts_seen = Queue.new
    huge = "data: " + ("x" * 5000) # no terminating blank line; exceeds the cap
    transport = FakeStreamTransport.new { |_| FakeStreamConnection.new([huge]) }
    client = Feat::StreamingClient.new(
      url: "https://example.test", api_key: "k", transport: transport,
      on_put: ->(p) { puts_seen << p }, on_error: ->(e) { errors << e },
      initial_backoff: 0.01, max_backoff: 0.05, max_event_bytes: 1024
    )
    client.start

    err = wait_until { errors.empty? ? nil : errors.pop }
    assert_instance_of Feat::SSEOverflowError, err
    assert puts_seen.empty?, "oversized payload must not be dispatched as a put"
  ensure
    client&.stop
  end

  def test_on_error_that_raises_does_not_kill_stream_thread
    puts_seen = Queue.new
    raised = Queue.new
    transport = FakeStreamTransport.new do |attempt|
      if attempt == 1
        RaisingStreamConnection.new(Feat::StreamError.new("boom"))
      else
        FakeStreamConnection.new([put_frame(4)])
      end
    end
    client = Feat::StreamingClient.new(
      url: "https://example.test", api_key: "k", transport: transport,
      on_put: ->(p) { puts_seen << p },
      on_error: ->(_) { raised << :raised; raise "callback blew up" },
      initial_backoff: 0.01, max_backoff: 0.05
    )
    client.start

    raised.pop # first failure triggers the throwing callback
    parsed = wait_until { puts_seen.empty? ? nil : puts_seen.pop }
    assert_equal 4, parsed["version"], "a raising on_error must not kill the stream thread"
  ensure
    client&.stop
  end

  def test_malformed_data_is_ignored
    puts_seen = Queue.new
    errors = Queue.new
    bad_then_good = "event: put\ndata: {not json}\n\nevent: put\ndata: {\"version\":3}\n\n"
    transport = FakeStreamTransport.new { |_| FakeStreamConnection.new([bad_then_good]) }
    client = build(transport, puts_seen: puts_seen, errors: errors)
    client.start

    parsed = wait_until { puts_seen.empty? ? nil : puts_seen.pop }
    assert_equal 3, parsed["version"], "malformed frame should be skipped, next one adopted"
  ensure
    client&.stop
  end
end
