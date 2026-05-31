# feathq-ruby-sdk

Server-side Ruby SDK for [feat](https://feat.so) feature flags. Local flag evaluation against a polled datafile. Standard library only — no gem dependencies.

## Install

```ruby
# Gemfile
gem "feathq-ruby-sdk"
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
  data_plane_url: "https://data.feat.so",
)
client.start

result = client.evaluate(
  "checkout-v2",
  default_value: false,
  context: {
    targetingKey: "user-123",
    user: { plan: "pro", email: "alice@example.com" },
  },
)

if result.value
  # ...
end

client.close
```

Use a **server** API key (`feat_sdk_...`).

## How it works

- Fetches a per-environment datafile and keeps it in memory.
- Polls every 30 seconds by default. ETag-aware via `If-None-Match`.
- Evaluation runs in-process: no per-flag network call.
- A background thread handles polling; `close` stops it cleanly.

## License

MIT
