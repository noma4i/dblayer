public enum GraphQLNullable<Wrapped: Encodable & Sendable & Equatable>: Sendable, Equatable {
  case omitted
  case null
  case value(Wrapped)
}

public extension KeyedEncodingContainer {
  mutating func encode(
    _ value: GraphQLNullable<some Encodable & Sendable & Equatable>,
    forKey key: Key
  ) throws {
    switch value {
    case .omitted:
      break
    case .null:
      try encodeNil(forKey: key)
    case let .value(wrapped):
      try encode(wrapped, forKey: key)
    }
  }
}
