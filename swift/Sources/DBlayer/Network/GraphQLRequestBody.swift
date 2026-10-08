
import Foundation

public enum GraphQLRequestBody {
  public static let persistedQueryVersion = 1

  public static func make(
    operationName: String,
    documentHash: String,
    variablesJSON: Data,
    document: String?
  ) -> Data? {
    guard let variables = try? JSONValue.decode(variablesJSON) else {
      return nil
    }

    var body: [String: JSONValue] = [
      "extensions": .object([
        "persistedQuery": .object([
          "sha256Hash": .string(documentHash),
          "version": .number(Double(persistedQueryVersion))
        ])
      ]),
      "operationName": .string(operationName),
      "variables": variables
    ]

    if let document, !document.isEmpty {
      body["query"] = .string(document)
    }

    return CanonicalJSON.encode(.object(body))
  }
}
