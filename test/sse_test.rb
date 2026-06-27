require "minitest/autorun"
require "feat"

# Unit tests for the incremental SSE parser.
class SSETest < Minitest::Test
  def collect(parser, chunk)
    events = []
    parser.feed(chunk) { |event| events << event }
    events
  end

  def test_parses_single_event
    parser = Feat::SSEParser.new
    events = collect(parser, "event: put\nid: 7\ndata: {\"version\":7}\n\n")
    assert_equal 1, events.length
    assert_equal "put", events[0][:event]
    assert_equal "7", events[0][:id]
    assert_equal "{\"version\":7}", events[0][:data]
  end

  def test_strips_only_one_leading_space
    parser = Feat::SSEParser.new
    events = collect(parser, "data:  two-spaces\n\n")
    assert_equal " two-spaces", events[0][:data]
  end

  def test_ignores_heartbeat_comments
    parser = Feat::SSEParser.new
    events = []
    parser.feed(":\n") { |e| events << e }
    parser.feed(": keep-alive\n") { |e| events << e }
    parser.feed("event: put\ndata: {}\n\n") { |e| events << e }
    assert_equal 1, events.length
    assert_equal "put", events[0][:event]
  end

  def test_joins_multiple_data_lines
    parser = Feat::SSEParser.new
    events = collect(parser, "data: a\ndata: b\n\n")
    assert_equal "a\nb", events[0][:data]
  end

  def test_handles_event_split_across_chunks
    parser = Feat::SSEParser.new
    events = []
    parser.feed("event: pu") { |e| events << e }
    parser.feed("t\ndata: {\"version") { |e| events << e }
    parser.feed("\":9}\n") { |e| events << e }
    assert_empty events
    parser.feed("\n") { |e| events << e }
    assert_equal 1, events.length
    assert_equal "put", events[0][:event]
    assert_equal "{\"version\":9}", events[0][:data]
  end

  def test_parses_back_to_back_events
    parser = Feat::SSEParser.new
    events = collect(parser, "event: put\ndata: 1\n\nevent: put\ndata: 2\n\n")
    assert_equal 2, events.length
    assert_equal "1", events[0][:data]
    assert_equal "2", events[1][:data]
  end

  def test_raises_when_unterminated_line_exceeds_cap
    parser = Feat::SSEParser.new(max_event_bytes: 64)
    assert_raises(Feat::SSEOverflowError) do
      parser.feed("data: " + ("x" * 200)) { |_| }
    end
  end

  def test_raises_when_accumulated_data_exceeds_cap
    parser = Feat::SSEParser.new(max_event_bytes: 64)
    assert_raises(Feat::SSEOverflowError) do
      10.times { parser.feed("data: #{"y" * 20}\n") { |_| } }
    end
  end

  def test_within_cap_does_not_raise
    parser = Feat::SSEParser.new(max_event_bytes: 1024)
    events = collect(parser, "event: put\ndata: {\"version\":1}\n\n")
    assert_equal 1, events.length
  end

  def test_tolerates_crlf_line_endings
    parser = Feat::SSEParser.new
    events = collect(parser, "event: put\r\ndata: ok\r\n\r\n")
    assert_equal 1, events.length
    assert_equal "ok", events[0][:data]
  end
end
