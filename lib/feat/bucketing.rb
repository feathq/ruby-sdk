require "digest"

module Feat
  # Deterministic bucketing for percentage rollouts. Algorithm matches
  # @feathq/feat-eval (JS), feat-go-sdk (Go), and feat (Python)
  # bit-for-bit:
  #
  #   sha1(salt + "." + flag_key + "." + context_key)
  #   -> first 8 bytes as big-endian uint64
  #   -> shift right 4 (drop low bits) for exactly 60 bits
  #   -> modulo 100_000
  module Bucketing
    BUCKET_SCALE = 100_000

    module_function

    def bucket(salt:, flag_key:, context_key:)
      digest = Digest::SHA1.digest("#{salt}.#{flag_key}.#{context_key}")
      first8 = digest[0, 8].unpack1("Q>") # big-endian unsigned 64-bit
      sixty = first8 >> 4
      sixty % BUCKET_SCALE
    end

    # Walk cumulative weights; return the variation whose range contains
    # bucket_value. Defensive fallback to the last variation if upstream
    # weights underflow the scale.
    def pick_by_weight(bucket_value, variations)
      cumulative = 0
      variations.each do |v|
        cumulative += v.weight
        return v.variationId if bucket_value < cumulative
      end
      variations.empty? ? nil : variations.last.variationId
    end
  end
end
