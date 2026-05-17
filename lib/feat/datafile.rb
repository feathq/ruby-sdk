module Feat
  # Wire-format types. JSON field names mirror @feathq/datafile-schema
  # exactly (camelCase) — `from_json` keeps the hash keys, no rekeying.
  module Datafile
    VariationSpec      = Struct.new(:id, :name, :value, keyword_init: true)
    TargetSpec         = Struct.new(:contextKindKey, :contextKey, :variationId, keyword_init: true)
    ConditionSpec      = Struct.new(:attributePath, :operator, :values, keyword_init: true)
    ConditionGroupSpec = Struct.new(:conditions, keyword_init: true)
    RolloutVariation   = Struct.new(:variationId, :weight, keyword_init: true)
    Rollout            = Struct.new(:bucketingContextKindKey, :variations, keyword_init: true)
    RuleSpec           = Struct.new(
      :id, :bucketingContextKindKey, :variationId, :rollout, :groups,
      keyword_init: true
    )
    FlagSpec = Struct.new(
      :id, :key, :valueType, :salt, :archived, :isEnabled, :offVariationId,
      :defaultVariationId, :defaultRollout, :defaultBucketingContextKindKey,
      :variations, :targets, :rules,
      keyword_init: true
    )
    SegmentRuleSpec = Struct.new(:conditions, keyword_init: true)
    SegmentSpec     = Struct.new(:key, :rules, keyword_init: true)
    ContextKindSpec = Struct.new(
      :key, :availableForRules, :availableForExperiments,
      keyword_init: true
    )

    File = Struct.new(
      :schemaVersion, :envId, :envKey, :projectId, :version, :etag,
      :generatedAt, :flags, :segments, :contextKinds,
      keyword_init: true
    )

    # Parse a wire-format hash (typically JSON.parse output) into a
    # Datafile::File. Hash keys must already be string-keyed (default for
    # JSON.parse).
    def self.from_json(data)
      File.new(
        schemaVersion: data["schemaVersion"],
        envId:         data["envId"],
        envKey:        data["envKey"],
        projectId:     data["projectId"],
        version:       data["version"],
        etag:          data["etag"],
        generatedAt:   data["generatedAt"],
        flags:         data["flags"].each_with_object({}) { |(k, v), o| o[k] = build_flag(v) },
        segments:      (data["segments"] || {}).each_with_object({}) { |(k, v), o| o[k] = build_segment(v) },
        contextKinds:  (data["contextKinds"] || {}).each_with_object({}) { |(k, v), o| o[k] = ContextKindSpec.new(**symbolize(v)) }
      )
    end

    def self.build_flag(d)
      FlagSpec.new(
        id:                             d["id"],
        key:                            d["key"],
        valueType:                      d["valueType"],
        salt:                           d["salt"],
        archived:                       d["archived"],
        isEnabled:                      d["isEnabled"],
        offVariationId:                 d["offVariationId"],
        defaultVariationId:             d["defaultVariationId"],
        defaultRollout:                 build_rollout(d["defaultRollout"]),
        defaultBucketingContextKindKey: d["defaultBucketingContextKindKey"],
        variations:                     d["variations"].map { |v| VariationSpec.new(**symbolize(v)) },
        targets:                        d["targets"].map { |t| TargetSpec.new(**symbolize(t)) },
        rules:                          d["rules"].map { |r| build_rule(r) }
      )
    end

    def self.build_rule(d)
      RuleSpec.new(
        id:                      d["id"],
        bucketingContextKindKey: d["bucketingContextKindKey"],
        variationId:             d["variationId"],
        rollout:                 build_rollout(d["rollout"]),
        groups:                  d["groups"].map { |g|
          ConditionGroupSpec.new(conditions: g["conditions"].map { |c| ConditionSpec.new(**symbolize(c)) })
        }
      )
    end

    def self.build_rollout(d)
      return nil if d.nil?

      Rollout.new(
        bucketingContextKindKey: d["bucketingContextKindKey"],
        variations:              d["variations"].map { |v| RolloutVariation.new(**symbolize(v)) }
      )
    end

    def self.build_segment(d)
      SegmentSpec.new(
        key:   d["key"],
        rules: d["rules"].map { |r|
          SegmentRuleSpec.new(conditions: r["conditions"].map { |c| ConditionSpec.new(**symbolize(c)) })
        }
      )
    end

    def self.symbolize(h)
      h.each_with_object({}) { |(k, v), o| o[k.to_sym] = v }
    end
  end
end
