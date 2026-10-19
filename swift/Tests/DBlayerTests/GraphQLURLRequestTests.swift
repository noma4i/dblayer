import Foundation
import Testing
@testable import DBlayer

@Suite(.serialized)
struct GraphQLURLRequestTests {
  @Test
  func buildsPostWithJsonBodyHeadersAndTimeout() throws {
    let url = try #require(URL(string: "https://example.test/"))
    let body = Data("{\"a\":1}".utf8)
    let request = GraphQLURLRequest.make(
      url: url,
      body: body,
      headers: ["X-API-Version": "2", "x-device-uuid": "device-1", "authorization": "Bearer tok"]
    )

    #expect(request.httpMethod == "POST")
    #expect(request.httpBody == body)
    #expect(request.url == url)
    #expect(request.timeoutInterval == 30)
    #expect(request.value(forHTTPHeaderField: "Content-Type") == "application/json")
    #expect(request.value(forHTTPHeaderField: "X-API-Version") == "2")
    #expect(request.value(forHTTPHeaderField: "x-device-uuid") == "device-1")
    #expect(request.value(forHTTPHeaderField: "authorization") == "Bearer tok")

    let overridden = GraphQLURLRequest.make(
      url: url,
      body: Data(),
      headers: ["Content-Type": "application/graphql"],
      timeout: 60
    )

    #expect(overridden.value(forHTTPHeaderField: "Content-Type") == "application/graphql")
    #expect(overridden.timeoutInterval == 60)
  }
}
