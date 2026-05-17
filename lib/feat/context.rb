module Feat
  # SDK-consumer-supplied context. Mirrors OpenFeature's pattern: a
  # `targeting_key` shorthand for "user.key", and a `kinds` hash whose
  # keys match the datafile's `contextKinds` map.
  #
  # Example:
  #
  #   Feat::EvalContext.new(
  #     targeting_key: "user-123",
  #     kinds: {
  #       "user"         => { "key" => "user-123", "email" => "u@example.com" },
  #       "organization" => { "key" => "acme",     "plan"  => "pro" }
  #     }
  #   )
  EvalContext = Struct.new(:targeting_key, :kinds, keyword_init: true) do
    def initialize(targeting_key: nil, kinds: {})
      super
    end
  end

  module ContextResolver
    module_function

    # Walks "user.email", "organization.plan", "user.address.city"
    # against the EvalContext. Returns nil if any segment is missing
    # — operators treat nil as a non-match.
    def resolve_attribute(ctx, attribute_path)
      return nil if attribute_path.nil? || attribute_path.empty?

      parts = attribute_path.split(".", 2)
      kind_obj = read_kind(ctx, parts[0])
      return nil if kind_obj.nil?
      return kind_obj["key"] if parts.length == 1

      rest = parts[1]
      cur = kind_obj
      rest.split(".").each do |p|
        return nil unless cur.is_a?(Hash)
        return nil unless cur.key?(p)

        cur = cur[p]
      end
      cur
    end

    def read_context_key(ctx, kind_key)
      obj = read_kind(ctx, kind_key)
      return nil if obj.nil?

      key = obj["key"]
      key.is_a?(String) ? key : nil
    end

    def read_kind(ctx, kind_key)
      if kind_key == "user"
        obj = ctx.kinds["user"]
        return obj if obj.is_a?(Hash)
        return { "key" => ctx.targeting_key } if ctx.targeting_key

        return nil
      end
      obj = ctx.kinds[kind_key]
      obj.is_a?(Hash) ? obj : nil
    end
  end
end
