<p align="center">
  <a href="https://feat.so">
    <img src="https://feat.so/logo/wordmark.png" alt="feat.so" width="320" />
  </a>
</p>

---

# feat Ruby SDK

Server-side Ruby SDK for [feat](https://feat.so) feature flags. Local flag evaluation against a live-streamed datafile. Standard library only - no gem dependencies.

## Install

```ruby
# Gemfile
gem "feat-sdk"
```

```bash
bundle install
```

Ruby 3.0+.

## Usage

```ruby
require "feat"

client = Feat::Client.new(
  api_key: ENV.fetch("FEAT_SERVER_KEY"),
  url: "https://data-01.feat.so", # optional; this is the default
)
client.start

ctx = {
  targetingKey: "user-123",
  user: { plan: "pro", email: "alice@example.com" },
}

if client.get_boolean_value("checkout-v2", false, ctx)
  # ...
end

client.close
```

Use a **server** API key (`feat_sdk_...`).

## How it works

- Fetches a per-environment datafile and keeps it in memory.
- Streams updates over Server-Sent Events: a background thread holds an open
  connection and applies each new datafile the instant it changes. Updates are
  version-ordered (an older or equal datafile is never adopted) and guarded by a
  mutex shared with the evaluator.
- Keeps a slow background poll (every 5 minutes by default) as a safety net.
  ETag-aware via `If-None-Match`. If the stream drops, it reconnects with
  exponential backoff while the poll keeps the datafile fresh.
- Evaluation runs in-process: no per-flag network call.
- `close` stops the stream and poll threads cleanly.

### Streaming options

Streaming is on by default. To disable it and rely on polling alone:

```ruby
client = Feat::Client.new(
  api_key: ENV.fetch("FEAT_SERVER_KEY"),
  streaming: false,        # poll-only mode
  poll_interval: 30,       # seconds (used when streaming is off)
)
```

When streaming is on, `safety_poll_interval:` (default 300 seconds) controls the
backstop poll cadence.

## License

MIT
