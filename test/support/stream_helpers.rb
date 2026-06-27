require "uri"

# Test doubles for the SSE transport seam used by Feat::StreamingClient and
# Feat::Client. No network, fully deterministic.

# Yields a fixed list of byte chunks, then blocks until #close is called
# (mimicking a server that holds the stream open).
class FakeStreamConnection
  attr_reader :closed

  def initialize(chunks)
    @chunks = chunks
    @gate   = Queue.new
    @closed = false
  end

  def each_chunk
    @chunks.each { |chunk| yield chunk }
    @gate.pop # block like a live stream until closed
  end

  def close
    @closed = true
    @gate.push(:close)
  rescue ThreadError
    nil
  end
end

# Yields its chunks then returns immediately, mimicking a server that seeds
# data on connect and drops the stream without holding it open. Used to
# exercise the min-uptime backoff guard.
class DroppingStreamConnection
  attr_reader :closed

  def initialize(chunks)
    @chunks = chunks
    @closed = false
  end

  def each_chunk
    @chunks.each { |chunk| yield chunk }
  end

  def close
    @closed = true
  end
end

# Connection that raises the moment it is read, to exercise reconnect paths.
class RaisingStreamConnection
  def initialize(error)
    @error = error
  end

  def each_chunk
    raise @error
  end

  def close; end
end

# Records connect attempts and delegates to a block that decides what each
# attempt produces (a connection, or a raise).
class FakeStreamTransport
  attr_reader :connect_count, :headers_seen, :uris_seen, :connect_times

  def initialize(&behavior)
    @behavior      = behavior
    @connect_count = 0
    @headers_seen  = []
    @uris_seen     = []
    @connect_times = []
    @mutex         = Mutex.new
  end

  def connect(uri:, headers:)
    attempt = nil
    @mutex.synchronize do
      @connect_count += 1
      @headers_seen << headers
      @uris_seen << uri
      @connect_times << Process.clock_gettime(Process::CLOCK_MONOTONIC)
      attempt = @connect_count
    end
    @behavior.call(attempt)
  end
end

# Minimal stand-in for the polling Net::HTTP seam (Client's http_client:).
# Serves a queue of canned responses, one per request.
class FakeHTTPResponse
  def initialize(code:, body:, headers: {})
    @code    = code.to_s
    @body    = body
    @headers = headers
  end

  attr_reader :code, :body

  def [](name)
    @headers[name]
  end
end

class FakeHTTPClient
  def initialize(responses)
    @responses = responses
    @mutex     = Mutex.new
  end

  def start(*_args, **_kwargs)
    yield self
  end

  def request(_req)
    @mutex.synchronize do
      raise "FakeHTTPClient: no more responses queued" if @responses.empty?

      @responses.shift
    end
  end
end

# Polling http_client that records each request's If-None-Match header and
# serves a queued response per request. Lets a test assert conditional-GET
# freshness: the poll after a patch carries the patched etag.
class RecordingHTTPClient
  attr_reader :if_none_match_seen

  def initialize(responses)
    @responses          = responses
    @if_none_match_seen = []
    @mutex              = Mutex.new
  end

  def start(*_args, **_kwargs)
    yield self
  end

  def request(req)
    @mutex.synchronize do
      @if_none_match_seen << req["If-None-Match"]
      raise "RecordingHTTPClient: no more responses queued" if @responses.empty?

      @responses.shift
    end
  end
end

# Polling http_client whose Nth request blocks until released, so a test can
# park the poll thread mid-fetch_once and verify close() joins it. The first
# request (the synchronous initial refresh) returns immediately.
class BlockingHTTPClient
  def initialize(first_body:, headers: {}, block_on: 2)
    @first    = FakeHTTPResponse.new(code: 200, body: first_body, headers: headers)
    @block_on = block_on
    @entered  = Queue.new # signals the test that a blocking request started
    @release  = Queue.new # the test pushes here to let the request return
    @count    = 0
    @mutex    = Mutex.new
  end

  attr_reader :entered

  def release!
    @release << :go
  end

  def start(*_args, **_kwargs)
    yield self
  end

  def request(_req)
    n = @mutex.synchronize { @count += 1 }
    return @first if n < @block_on

    @entered << :now
    @release.pop
    @first
  end
end

# Spin until the block is truthy or the timeout elapses. Returns the value.
def wait_until(timeout: 2.0, interval: 0.01)
  deadline = Process.clock_gettime(Process::CLOCK_MONOTONIC) + timeout
  loop do
    value = yield
    return value if value

    break if Process.clock_gettime(Process::CLOCK_MONOTONIC) > deadline

    sleep interval
  end
  yield
end
