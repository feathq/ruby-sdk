require_relative "lib/feat/version"

Gem::Specification.new do |spec|
  spec.name          = "feat-sdk"
  spec.version       = Feat::VERSION
  spec.authors       = ["feat HQ"]
  spec.email         = ["support@feat.so"]
  spec.summary       = "feat feature-flag SDK for Ruby (server-side, local evaluation)"
  spec.description   = "Server-side Ruby SDK for feat. Polls a per-environment datafile and evaluates flags locally with no per-flag network call. Stdlib only."
  spec.homepage      = "https://feat.so"
  spec.license       = "MIT"
  spec.required_ruby_version = ">= 3.0"
  spec.files         = Dir.glob("lib/**/*.rb") + ["README.md", "LICENSE"]
  spec.require_paths = ["lib"]

  spec.metadata["homepage_uri"]    = spec.homepage
  spec.metadata["source_code_uri"] = "https://github.com/feathq/ruby-sdk"
  spec.metadata["bug_tracker_uri"] = "https://github.com/feathq/ruby-sdk/issues"
  spec.metadata["changelog_uri"]   = "https://github.com/feathq/ruby-sdk/releases"
end
