require "minitest/autorun"
require "feat"

# Parity suite — mirrors test/eval.test.ts (JS), feat/eval_test.go (Go),
# and tests/test_eval.py (Python). New cases land in all four so eval
# semantics stay aligned across SDKs.
class EvalTest < Minitest::Test
  TRUE_VAR  = Feat::Datafile::VariationSpec.new(id: "var-true",  name: "true",  value: true)
  FALSE_VAR = Feat::Datafile::VariationSpec.new(id: "var-false", name: "false", value: false)

  def make_df(flags: {}, segments: {})
    Feat::Datafile::File.new(
      schemaVersion: 1,
      envId:         "env-1",
      envKey:        "staging",
      projectId:     "proj-1",
      version:       1,
      etag:          "etag",
      generatedAt:   "2026-05-17T00:00:00Z",
      flags:         flags,
      segments:      segments,
      contextKinds: {
        "user" => Feat::Datafile::ContextKindSpec.new(key: "user", availableForRules: true, availableForExperiments: true)
      }
    )
  end

  def bool_flag(**overrides)
    base = {
      id:                             "flag-1",
      key:                            "checkout",
      valueType:                      "boolean",
      salt:                           "abcdef0123456789",
      archived:                       false,
      isEnabled:                      true,
      offVariationId:                 FALSE_VAR.id,
      defaultVariationId:             FALSE_VAR.id,
      defaultRollout:                 nil,
      defaultBucketingContextKindKey: nil,
      variations:                     [TRUE_VAR, FALSE_VAR],
      targets:                        [],
      rules:                          []
    }
    Feat::Datafile::FlagSpec.new(**base.merge(overrides))
  end

  def user_ctx(key, **attrs)
    kind = { "key" => key }.merge(attrs.transform_keys(&:to_s))
    Feat::EvalContext.new(kinds: { "user" => kind })
  end

  def eval(flag_key, default, ctx, df)
    Feat::Eval.call(flag_key: flag_key, default_value: default, ctx: ctx, datafile: df)
  end

  def test_archived_returns_off
    df = make_df(flags: { "checkout" => bool_flag(archived: true) })
    r = eval("checkout", false, user_ctx("u1"), df)
    assert_equal false, r.value
    assert_equal Feat::Reason::DISABLED, r.reason
  end

  def test_disabled_returns_off
    df = make_df(flags: { "checkout" => bool_flag(isEnabled: false) })
    r = eval("checkout", true, user_ctx("u1"), df)
    assert_equal false, r.value
    assert_equal Feat::Reason::DISABLED, r.reason
  end

  def test_default_when_no_targeting
    df = make_df(flags: { "checkout" => bool_flag })
    r = eval("checkout", true, user_ctx("u1"), df)
    assert_equal false, r.value
    assert_equal Feat::Reason::FALLTHROUGH, r.reason
  end

  def test_individual_target_beats_rules
    flag = bool_flag(
      targets: [Feat::Datafile::TargetSpec.new(contextKindKey: "user", contextKey: "u-vip", variationId: TRUE_VAR.id)],
      rules: [
        Feat::Datafile::RuleSpec.new(
          id:                      "r1",
          bucketingContextKindKey: nil,
          variationId:             FALSE_VAR.id,
          rollout:                 nil,
          groups: [
            Feat::Datafile::ConditionGroupSpec.new(conditions: [
              Feat::Datafile::ConditionSpec.new(attributePath: "user.key", operator: "is_one_of", values: ["u-vip"])
            ])
          ]
        )
      ]
    )
    df = make_df(flags: { "checkout" => flag })
    r = eval("checkout", false, user_ctx("u-vip"), df)
    assert_equal true, r.value
    assert_equal Feat::Reason::TARGETING_MATCH, r.reason
  end

  def test_rule_ends_with_email
    flag = bool_flag(rules: [
      Feat::Datafile::RuleSpec.new(
        id:                      "r1",
        bucketingContextKindKey: nil,
        variationId:             TRUE_VAR.id,
        rollout:                 nil,
        groups: [
          Feat::Datafile::ConditionGroupSpec.new(conditions: [
            Feat::Datafile::ConditionSpec.new(attributePath: "user.email", operator: "ends_with", values: ["@example.com"])
          ])
        ]
      )
    ])
    df = make_df(flags: { "checkout" => flag })
    r = eval("checkout", false, user_ctx("u1", email: "alice@example.com"), df)
    assert_equal true, r.value
    assert_equal Feat::Reason::TARGETING_MATCH, r.reason
  end

  def test_rule_or_groups
    flag = bool_flag(rules: [
      Feat::Datafile::RuleSpec.new(
        id: "r1", bucketingContextKindKey: nil, variationId: TRUE_VAR.id, rollout: nil,
        groups: [
          Feat::Datafile::ConditionGroupSpec.new(conditions: [
            Feat::Datafile::ConditionSpec.new(attributePath: "user.email", operator: "ends_with", values: ["@nope.com"])
          ]),
          Feat::Datafile::ConditionGroupSpec.new(conditions: [
            Feat::Datafile::ConditionSpec.new(attributePath: "user.plan", operator: "is_one_of", values: ["pro", "enterprise"])
          ])
        ]
      )
    ])
    df = make_df(flags: { "checkout" => flag })
    r = eval("checkout", false, user_ctx("u1", email: "x@elsewhere.com", plan: "pro"), df)
    assert_equal true, r.value
  end

  def test_rollout_deterministic
    flag = bool_flag(
      defaultVariationId: nil,
      defaultRollout: Feat::Datafile::Rollout.new(
        bucketingContextKindKey: "user",
        variations: [
          Feat::Datafile::RolloutVariation.new(variationId: TRUE_VAR.id,  weight: 50_000),
          Feat::Datafile::RolloutVariation.new(variationId: FALSE_VAR.id, weight: 50_000)
        ]
      )
    )
    df = make_df(flags: { "checkout" => flag })
    r1 = eval("checkout", false, user_ctx("stable-key"), df)
    r2 = eval("checkout", false, user_ctx("stable-key"), df)
    assert_equal r1.value, r2.value
    assert_equal Feat::Reason::SPLIT, r1.reason
  end

  def test_rollout_100_percent
    flag = bool_flag(
      defaultVariationId: nil,
      defaultRollout: Feat::Datafile::Rollout.new(
        bucketingContextKindKey: "user",
        variations: [Feat::Datafile::RolloutVariation.new(variationId: TRUE_VAR.id, weight: 100_000)]
      )
    )
    df = make_df(flags: { "checkout" => flag })
    %w[u1 u2 u3 u4 u5].each do |k|
      r = eval("checkout", false, user_ctx(k), df)
      assert_equal true, r.value, "expected true for #{k}"
    end
  end

  def test_segment_match
    flag = bool_flag(rules: [
      Feat::Datafile::RuleSpec.new(
        id: "r1", bucketingContextKindKey: nil, variationId: TRUE_VAR.id, rollout: nil,
        groups: [
          Feat::Datafile::ConditionGroupSpec.new(conditions: [
            Feat::Datafile::ConditionSpec.new(attributePath: "", operator: "segment_match", values: ["internal-users"])
          ])
        ]
      )
    ])
    segs = {
      "internal-users" => Feat::Datafile::SegmentSpec.new(
        key: "internal-users",
        rules: [
          Feat::Datafile::SegmentRuleSpec.new(conditions: [
            Feat::Datafile::ConditionSpec.new(attributePath: "user.email", operator: "ends_with", values: ["@feathq.com"])
          ])
        ]
      )
    }
    df = make_df(flags: { "checkout" => flag }, segments: segs)

    hit  = eval("checkout", false, user_ctx("u1", email: "bob@feathq.com"), df)
    assert_equal true, hit.value
    miss = eval("checkout", false, user_ctx("u2", email: "bob@other.com"), df)
    assert_equal false, miss.value
  end

  def test_semver_gte
    flag = bool_flag(rules: [
      Feat::Datafile::RuleSpec.new(
        id: "r1", bucketingContextKindKey: nil, variationId: TRUE_VAR.id, rollout: nil,
        groups: [
          Feat::Datafile::ConditionGroupSpec.new(conditions: [
            Feat::Datafile::ConditionSpec.new(attributePath: "user.app_version", operator: "semver_gte", values: ["1.2.0"])
          ])
        ]
      )
    ])
    df = make_df(flags: { "checkout" => flag })
    newer = eval("checkout", false, user_ctx("u1", app_version: "1.5.0"), df)
    assert_equal true, newer.value
    older = eval("checkout", false, user_ctx("u2", app_version: "1.1.5"), df)
    assert_equal false, older.value
  end

  def test_missing_flag_returns_error
    df = make_df
    r = eval("missing", "fallback", user_ctx("u1"), df)
    assert_equal Feat::Reason::ERROR, r.reason
    assert_equal "fallback", r.value
  end
end
