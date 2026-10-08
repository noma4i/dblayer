import Foundation
import Testing
@testable import DBlayer

@Suite(.serialized)
struct GraphQLRequestBodyTests {
  @Test
  func firstAttemptOmitsQueryAndRetryAddsIt() throws {
    let first = GraphQLRequestBody.make(
      operationName: "Ping",
      documentHash: "abc123",
      variablesJSON: Data("{}".utf8),
      document: nil
    )
    let retry = GraphQLRequestBody.make(
      operationName: "Ping",
      documentHash: "abc123",
      variablesJSON: Data("{}".utf8),
      document: "query Ping { ping }"
    )

    #expect(
      try string(from: first) ==
        "{\"extensions\":{\"persistedQuery\":{\"sha256Hash\":\"abc123\",\"version\":1}},\"operationName\":\"Ping\",\"variables\":{}}"
    )
    #expect(
      try string(from: retry) ==
        "{\"extensions\":{\"persistedQuery\":{\"sha256Hash\":\"abc123\",\"version\":1}}," +
        "\"operationName\":\"Ping\",\"query\":\"query Ping { ping }\",\"variables\":{}}"
    )
  }

  @Test
  func keepsVariablesTreeAndRejectsBrokenVariables() throws {
    let body = GraphQLRequestBody.make(
      operationName: "Ping",
      documentHash: "abc123",
      variablesJSON: Data("{\"b\":2,\"a\":[1,{\"c\":null}]}".utf8),
      document: nil
    )
    let broken = GraphQLRequestBody.make(
      operationName: "Ping",
      documentHash: "abc123",
      variablesJSON: Data("{oops".utf8),
      document: nil
    )

    #expect(try string(from: body).contains("\"variables\":{\"a\":[1,{\"c\":null}],\"b\":2}"))
    #expect(broken == nil)
  }

  private func string(from result: Data?) throws -> String {
    let data = try #require(result)
    return try #require(String(data: data, encoding: .utf8))
  }
}
