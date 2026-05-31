require "date"
require "time"

module Feat
  # Per-operator predicates. Defensive: type-mismatch / parse-failure
  # returns false rather than raising — matches the JS engine's posture
  # against malformed contexts at the edge.
  #
  # segment_match / segment_not_match are dispatched by the rule
  # evaluator (they recurse into the datafile's segments map), not here.
  module Operators
    module_function

    def match(operator, lhs, values)
      case operator
      when "is_one_of"        then any_eq?(lhs, values)
      when "is_not_one_of"    then !any_eq?(lhs, values)
      when "is_empty"         then empty?(lhs)
      when "is_not_empty"     then !empty?(lhs)
      when "contains"         then string_any?(lhs, values) { |s, v| s.include?(v) }
      when "does_not_contain"
        return true unless lhs.is_a?(String)

        !string_any?(lhs, values) { |s, v| s.include?(v) }
      when "starts_with"      then string_any?(lhs, values) { |s, v| s.start_with?(v) }
      when "ends_with"        then string_any?(lhs, values) { |s, v| s.end_with?(v) }
      when "matches_regex"
        return false unless lhs.is_a?(String)

        any_string_value?(values) { |v|
          next false unless safe_regex?(v)

          begin
            !!(lhs =~ Regexp.new(v))
          rescue RegexpError
            false
          end
        }
      when "gt"               then numeric_cmp(lhs, values) { |a, b| a > b }
      when "gte"              then numeric_cmp(lhs, values) { |a, b| a >= b }
      when "lt"               then numeric_cmp(lhs, values) { |a, b| a < b }
      when "lte"              then numeric_cmp(lhs, values) { |a, b| a <= b }
      when "before"           then date_cmp(lhs, values) { |a, b| a < b }
      when "after"            then date_cmp(lhs, values) { |a, b| a > b }
      when "semver_eq"        then semver_cmp(lhs, values) { |c| c.zero? }
      when "semver_gt"        then semver_cmp(lhs, values) { |c| c.positive? }
      when "semver_gte"       then semver_cmp(lhs, values) { |c| c >= 0 }
      when "semver_lt"        then semver_cmp(lhs, values) { |c| c.negative? }
      when "semver_lte"       then semver_cmp(lhs, values) { |c| c <= 0 }
      when "segment_match", "segment_not_match"
        false
      else
        false
      end
    end

    def empty?(lhs)
      lhs.nil? || lhs == ""
    end

    # JS-engine-compatible equality with string/number coercion.
    def deep_eq(a, b)
      return true if a == b

      if a.is_a?(Numeric) && b.is_a?(String)
        return a.to_s == b || float_eq(a, b)
      end
      if a.is_a?(String) && b.is_a?(Numeric)
        return a == b.to_s || float_eq(b, a)
      end

      false
    end

    def float_eq(num, str)
      Float(str) == num.to_f
    rescue ArgumentError, TypeError
      false
    end

    def any_eq?(lhs, values)
      values.any? { |v| deep_eq(lhs, v) }
    end

    def string_any?(lhs, values)
      return false unless lhs.is_a?(String)

      values.any? { |v| v.is_a?(String) && yield(lhs, v) }
    end

    def any_string_value?(values)
      values.any? { |v| v.is_a?(String) && yield(v) }
    end

    def numeric_cmp(lhs, values)
      a = to_number(lhs)
      return false if a.nil?

      values.any? do |v|
        b = to_number(v)
        b && yield(a, b)
      end
    end

    def to_number(x)
      case x
      when Numeric then x.to_f
      when String
        Float(x)
      end
    rescue ArgumentError, TypeError
      nil
    end

    def date_cmp(lhs, values)
      a = to_time(lhs)
      return false if a.nil?

      values.any? do |v|
        b = to_time(v)
        b && yield(a, b)
      end
    end

    def to_time(x)
      case x
      when String then Time.iso8601(x)
      when Numeric then Time.at(x.to_f / 1000.0)
      end
    rescue ArgumentError
      nil
    end

    # ReDoS guard for matches_regex. Caps pattern length and rejects the
    # most common catastrophic-backtracking shapes. False positives just
    # turn the rule into a non-match, which is the safe default.
    REDOS_SHAPES = /\([^)]*[+*][^)]*\)\s*[+*]|\([^)]*\|[^)]*\)\s*[+*]/

    def safe_regex?(pattern)
      return false if pattern.length > 512
      return false if REDOS_SHAPES.match?(pattern)

      true
    end

    SEMVER_RE = /\A(\d+)\.(\d+)\.(\d+)(?:-([0-9A-Za-z.-]+))?(?:\+[0-9A-Za-z.-]+)?\z/

    def parse_semver(x)
      return nil unless x.is_a?(String)

      m = x.strip.match(SEMVER_RE)
      return nil if m.nil?

      [m[1].to_i, m[2].to_i, m[3].to_i, m[4]]
    end

    def compare_semver(a, b)
      3.times { |i| return a[i] - b[i] if a[i] != b[i] }
      ap, bp = a[3], b[3]
      return 0 if ap == bp
      return 1 if ap.nil?
      return -1 if bp.nil?

      ap <=> bp
    end

    def semver_cmp(lhs, values)
      a = parse_semver(lhs)
      return false if a.nil?

      values.any? do |v|
        b = parse_semver(v)
        b && yield(compare_semver(a, b))
      end
    end
  end
end
