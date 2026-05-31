module Feat
  # Evaluation result reasons. Match OpenFeature's enum + the
  # cross-language SDK convention.
  module Reason
    TARGETING_MATCH = "TARGETING_MATCH".freeze
    SPLIT           = "SPLIT".freeze
    FALLTHROUGH     = "FALLTHROUGH".freeze
    DEFAULT         = "DEFAULT".freeze
    DISABLED        = "DISABLED".freeze
    ERROR           = "ERROR".freeze
    STATIC          = "STATIC".freeze
  end

  EvaluationResult = Struct.new(:value, :variation_id, :reason, :error_message, keyword_init: true) do
    def initialize(value:, variation_id: nil, reason:, error_message: nil)
      super
    end
  end

  module Eval
    module_function

    # Run the precedence pipeline:
    #
    #   1. archived flag        -> off variation        DISABLED
    #   2. !isEnabled           -> off variation        DISABLED
    #   3. individual target    -> target variation     TARGETING_MATCH
    #   4. first matching rule  -> rule variation/rollout TARGETING_MATCH / SPLIT
    #   5. default              -> default variation/rollout FALLTHROUGH / SPLIT
    #   6. nothing matched      -> off variation        DEFAULT
    def call(flag_key:, default_value:, ctx:, datafile:)
      flag = datafile.flags[flag_key]
      if flag.nil?
        return EvaluationResult.new(
          value: default_value, reason: Reason::ERROR,
          error_message: "flag could not be evaluated"
        )
      end

      if flag.archived || !flag.isEnabled
        return resolve_variation(flag, flag.offVariationId, Reason::DISABLED, default_value)
      end

      flag.targets.each do |target|
        ctx_key = ContextResolver.read_context_key(ctx, target.contextKindKey)
        if !ctx_key.nil? && ctx_key == target.contextKey
          return resolve_variation(flag, target.variationId, Reason::TARGETING_MATCH, default_value)
        end
      end

      flag.rules.each do |rule|
        next unless match_rule?(rule, ctx, datafile)

        if !rule.variationId.nil?
          return resolve_variation(flag, rule.variationId, Reason::TARGETING_MATCH, default_value)
        end
        if !rule.rollout.nil?
          picked = pick_rollout(flag, rule.rollout, ctx)
          return resolve_variation(flag, picked, Reason::SPLIT, default_value) unless picked.nil?
        end
      end

      if !flag.defaultVariationId.nil?
        return resolve_variation(flag, flag.defaultVariationId, Reason::FALLTHROUGH, default_value)
      end
      if !flag.defaultRollout.nil?
        picked = pick_rollout(flag, flag.defaultRollout, ctx)
        return resolve_variation(flag, picked, Reason::SPLIT, default_value) unless picked.nil?
      end

      resolve_variation(flag, flag.offVariationId, Reason::DEFAULT, default_value)
    end

    def match_rule?(rule, ctx, datafile)
      return false if rule.groups.empty?

      rule.groups.any? do |group|
        next false if group.conditions.empty?

        group.conditions.all? { |cond| Segments.match_condition(cond, ctx, datafile) }
      end
    end

    def pick_rollout(flag, rollout, ctx)
      ctx_key = ContextResolver.read_context_key(ctx, rollout.bucketingContextKindKey)
      return nil if ctx_key.nil?

      b = Bucketing.bucket(salt: flag.salt, flag_key: flag.key, context_key: ctx_key)
      Bucketing.pick_by_weight(b, rollout.variations)
    end

    def resolve_variation(flag, variation_id, reason, default_value)
      v = flag.variations.find { |x| x.id == variation_id }
      if v.nil?
        return EvaluationResult.new(
          value: default_value, reason: Reason::ERROR,
          error_message: "flag could not be evaluated"
        )
      end
      EvaluationResult.new(value: v.value, variation_id: variation_id, reason: reason)
    end
  end
end
