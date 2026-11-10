import DBlayerTesting
import Foundation
import Testing
@testable import DBlayer

@Suite
struct ScriptedURLProtocolTests {
  @Test
  func returnsQueuedResponseAndRecordsSentRequest() async throws {
    let scriptId = UUID().uuidString
    ScriptedURLProtocol.reset(scriptId: scriptId)

    let url = try #require(URL(string: "https://example.test/"))
    let expectedBody = Data(#"{"ok":true}"#.utf8)
    let sentBody = Data(#"{"a":1}"#.utf8)
    ScriptedURLProtocol.push(
      scriptId: scriptId,
      status: 200,
      headers: ["x-test": "1"],
      body: expectedBody,
      url: url
    )
    var request = URLRequest(url: url)
    request.httpMethod = "POST"
    request.setValue("device-1", forHTTPHeaderField: "x-device-uuid")
    request.httpBody = sentBody

    let (body, response) = try await ScriptedURLProtocol.session(scriptId: scriptId).data(for: request)
    let httpResponse = try #require(response as? HTTPURLResponse)

    #expect(body == expectedBody)
    #expect(httpResponse.statusCode == 200)
    #expect(httpResponse.value(forHTTPHeaderField: "x-test") == "1")
    #expect(ScriptedURLProtocol.recordedRequests(scriptId: scriptId).count == 1)
    let recordedRequest = try #require(ScriptedURLProtocol.recordedRequests(scriptId: scriptId).first)
    #expect(recordedRequest.httpMethod == "POST")
    #expect(recordedRequest.value(forHTTPHeaderField: "x-device-uuid") == "device-1")
    #expect(ScriptedURLProtocol.body(of: recordedRequest) == sentBody)
  }

  @Test
  func failsWithoutQueuedResponseAndIsolatesSessionsByScriptId() async throws {
    let scriptId = UUID().uuidString
    ScriptedURLProtocol.reset(scriptId: scriptId)
    let first = UUID().uuidString
    let second = UUID().uuidString
    ScriptedURLProtocol.reset(scriptId: first)
    ScriptedURLProtocol.reset(scriptId: second)

    let url = try #require(URL(string: "https://example.test/"))
    await #expect(throws: URLError.self) {
      _ = try await ScriptedURLProtocol.session(scriptId: scriptId).data(from: url)
    }

    ScriptedURLProtocol.push(scriptId: first, status: 200, headers: [:], body: Data(), url: url)
    await #expect(throws: URLError.self) {
      _ = try await ScriptedURLProtocol.session(scriptId: second).data(from: url)
    }

    let (_, response) = try await ScriptedURLProtocol.session(scriptId: first).data(from: url)
    let httpResponse = try #require(response as? HTTPURLResponse)
    #expect(httpResponse.statusCode == 200)
    #expect(ScriptedURLProtocol.recordedRequests(scriptId: first).count == 1)
  }
}

