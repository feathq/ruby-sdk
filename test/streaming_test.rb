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
