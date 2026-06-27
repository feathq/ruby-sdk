module Feat
  # Raised when a single SSE event (its in-flight buffer plus accumulated
  # data) grows past the configured byte cap. Aborts the connection rather
  # than letting a missing newline or a giant data field exhaust memory.
  class SSEOverflowError < StandardError; end

  # Incremental Server-Sent Events parser. Pure: it does no IO.
  #
  # Feed raw response bytes with #feed; the parser buffers, splits on
  # newline boundaries, and yields one Hash per dispatched event:
  #
  #   { event: "put", data: "<json>", id: "42" }
  #
  # Per the SSE wire format:
  #   - "field: value" lines set the event/data/id of the pending event;
  #     a single leading space after the colon is stripped from the value.
  #   - "data:" lines accumulate and are joined with "\n".
  #   - A blank line dispatches the pending event.
  #   - Lines starting with ":" are comments (heartbeats) and are ignored.
  class SSEParser
    # Upper bound on the bytes held for one in-progress event. Mirrors
    # Client::MAX_DATAFILE_BYTES so the stream path is bounded the same way
    # the poll path is.
    MAX_EVENT_BYTES = 10 * 1024 * 1024

    def initialize(max_event_bytes: MAX_EVENT_BYTES)
      @max_event_bytes = max_event_bytes
      @buffer = +""
      reset_event
    end

    # Append a chunk of bytes and yield each fully-formed event.
    def feed(chunk)
      @buffer << chunk
      while (idx = @buffer.index("\n"))
        line = @buffer.slice!(0, idx + 1)
        # String#chomp strips a trailing "\r\n", "\n", or "\r".
        process_line(line.chomp) { |event| yield event }
      end
      # A line that never terminates, or a single oversized data field, must
      # not grow the buffers without bound. Abort once past the cap.
      guard_size!
    end

    private

    def guard_size!
      buffered = @buffer.bytesize
      @data.each { |segment| buffered += segment.bytesize }
      return if buffered <= @max_event_bytes

      raise SSEOverflowError, "SSE event exceeds #{@max_event_bytes} bytes"
    end

    def process_line(line)
      if line.empty?
        dispatch { |event| yield event }
        return
      end
      return if line.start_with?(":") # heartbeat / comment

      field, sep, value = line.partition(":")
      # A bare "field" with no colon is ignored (no value to set).
      return if sep.empty?

      value = value[1..] if value.start_with?(" ")
      case field
      when "event" then @event = value
      when "data"  then @data << value
      when "id"    then @id = value
      end
    end

    def dispatch
      # Nothing buffered between two blank lines -> nothing to emit.
      return if @event.nil? && @data.empty? && @id.nil?

      yield({ event: @event || "message", data: @data.join("\n"), id: @id })
      reset_event
    end

    def reset_event
      @event = nil
      @data = []
      @id = nil
    end
  end
end
