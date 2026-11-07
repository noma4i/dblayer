import Foundation

let rawReturns = #"{"name":"raw","kind":{"type":"raw"}}"#

func declaringReturns(_ defsJson: String) throws -> String {
  guard var defs = try JSONSerialization.jsonObject(with: Data(defsJson.utf8)) as? [[String: Any]] else { return defsJson }
  for index in defs.indices where defs[index]["returns"] == nil {
    defs[index]["returns"] = operationReturns[defs[index]["name"] as? String ?? ""] ?? rawReturns
  }
  return String(decoding: try JSONSerialization.data(withJSONObject: defs), as: UTF8.self)
}
