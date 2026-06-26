module Feat
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
    def initialize
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
    end

    private

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
