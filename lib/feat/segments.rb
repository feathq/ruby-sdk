module Feat
  module Segments
    module_function

    # True iff context matches the segment. Unknown segment keys
    # evaluate to false (never raise).
    def match_segment(segment_key, ctx, datafile)
      seg = datafile.segments[segment_key]
      return false if seg.nil?

      seg.rules.any? do |rule|
        next false if rule.conditions.empty?

        rule.conditions.all? { |cond| match_condition(cond, ctx, datafile) }
      end
    end

    def match_condition(cond, ctx, datafile)
      case cond.operator
      when "segment_match"
        keys = cond.values.select { |v| v.is_a?(String) }
        keys.any? { |k| match_segment(k, ctx, datafile) }
      when "segment_not_match"
        keys = cond.values.select { |v| v.is_a?(String) }
        !keys.any? { |k| match_segment(k, ctx, datafile) }
      else
        lhs = ContextResolver.resolve_attribute(ctx, cond.attributePath)
        Operators.match(cond.operator, lhs, cond.values)
      end
    end
  end
end
