require "feat/version"
require "feat/datafile"
require "feat/context"
require "feat/bucketing"
require "feat/operators"
require "feat/segments"
require "feat/eval"
require "feat/client"

# Feat — feature-flag SDK for Ruby.
#
# Server-side evaluation against a locally-cached datafile fetched from
# the feat data plane. Wire format mirrors @feathq/datafile-schema; eval
# precedence mirrors @feathq/feat-eval. New cases land in all four SDK
# test suites to keep semantics aligned.
module Feat
end
