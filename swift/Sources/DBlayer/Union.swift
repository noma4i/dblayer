import Foundation

public enum UnionTypenameKey: String, CodingKey { case typename = "__typename" }

extension JSONValue {
  public static func tagged<T: Encodable>(_ typename: String, _ value: T) throws -> JSONValue {
    var fields = try decodeObject(JSONEncoder().encode(value))
    fields["__typename"] = .string(typename)
    return .object(fields)
  }
}
