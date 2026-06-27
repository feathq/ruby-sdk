require "minitest/autorun"
require "feat"

# Unit tests for the pure datafile merge (Feat::Datafile.merge_patch). These
# exercise merge behaviour directly, without a running client or stream.
class DatafileTest < Minitest::Test
  def flag_hash
    {
      "id"                             => "flag-1",
      "key"                            => "checkout",
      "valueType"                      => "boolean",
      "salt"                           => "abcdef0123456789",
      "archived"                       => false,
      "isEnabled"                      => true,
      "offVariationId"                 => "off",
      "defaultVariationId"             => "on",
      "defaultRollout"                 => nil,
      "defaultBucketingContextKindKey" => nil,
      "variations"                     => [{ "id" => "on", "name" => "on", "value" => true }],
      "targets"                        => [],
      "rules"                          => [],
    }
  end

  def segment_hash(key)
    {
      "key"   => key,
      "rules" => [
        { "conditions" => [
          { "attributePath" => "email", "operator" => "ends_with", "values" => ["@feat.so"] },
        ] },
      ],
    }
  end

  def base(version: 1, segments: {})
    Feat::Datafile.from_json(
      "schemaVersion" => 1,
      "envId"         => "env-1",
      "envKey"        => "staging",
      "projectId"     => "proj-1",
      "version"       => version,
      "etag"          => "etag-#{version}",
      "generatedAt"   => "2026-06-26T00:00:00Z",
      "flags"         => { "checkout" => flag_hash },
      "segments"      => segments,
      "contextKinds"  => {}
    )
  end

  def test_merge_patch_adds_and_removes_segments
    current = base(segments: {
      "beta"     => segment_hash("beta"),
      "internal" => segment_hash("internal"),
    })
    patch = {
      "from"            => 1,
      "to"              => 2,
      "segments"        => { "vip" => segment_hash("vip") },
      "removedSegments" => ["internal"],
    }

    merged = Feat::Datafile.merge_patch(current, patch)

    assert_equal %w[beta vip], merged.segments.keys.sort, "added segment present, removed one dropped"
    assert_equal "vip", merged.segments["vip"].key, "added segment is built from its wire object"
    refute merged.segments.key?("internal"), "removedSegments must drop the segment"
    assert current.segments.key?("internal"), "merge_patch must not mutate the input datafile"
    assert_equal 2, merged.version
  end

  def test_merge_patch_takes_generatedat_from_patch
    merged = Feat::Datafile.merge_patch(base, "from" => 1, "to" => 2, "generatedAt" => "2026-07-01T12:00:00Z")
    assert_equal "2026-07-01T12:00:00Z", merged.generatedAt, "generatedAt advances to the patch value"
  end

  def test_merge_patch_generatedat_falls_back_to_current_when_omitted
    merged = Feat::Datafile.merge_patch(base, "from" => 1, "to" => 2)
    assert_equal "2026-06-26T00:00:00Z", merged.generatedAt, "generatedAt falls back to current when the patch omits it"
  end
end
