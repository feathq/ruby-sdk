Gem::Specification.new do |spec|
  spec.name          = "feat"
  spec.version       = "0.1.0"
  spec.authors       = ["feathq"]
  spec.summary       = "feat feature-flag SDK for Ruby (server-side, local evaluation)"
  spec.license       = "MIT"
  spec.required_ruby_version = ">= 3.0"
  spec.files         = Dir.glob("lib/**/*.rb")
  spec.require_paths = ["lib"]
end
