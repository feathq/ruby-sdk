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
  attr_reader :connect_count, :headers_seen, :uris_seen

  def initialize(&behavior)
    @behavior      = behavior
    @connect_count = 0
    @headers_seen  = []
    @uris_seen     = []
    @mutex         = Mutex.new
  end

  def connect(uri:, headers:)
    attempt = nil
    @mutex.synchronize do
      @connect_count += 1
      @headers_seen << headers
      @uris_seen << uri
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
