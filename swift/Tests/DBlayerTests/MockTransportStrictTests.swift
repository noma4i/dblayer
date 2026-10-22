import DBlayerTesting
import Foundation
import Testing
@testable import DBlayer

struct MockTransportStrictTests {
  private let request = TransportRequest(document: "query Ping { ping }", operationName: "Ping", variablesJson: #"{"page":2}"#)

  @Test
  func aCallWithoutAReplyFailsNamingTheOperationAndVariables() async {
    let transport = MockTransport()
    do {
      _ = try await transport.query(request: request)
      Issue.record("the call without a reply returned")
    } catch let DbError.Invalid(detail) {
      #expect(detail.contains("Ping") && detail.contains(#"{"page":2}"#))
    } catch {
      Issue.record("wrong error \(error)")
    }
    #expect(transport.unscripted.map(\.operationName) == ["Ping"])
    #expect(transport.calls.map(\.operationName) == ["Ping"])
  }

  @Test
  func aCallWithoutAReplyIsANetworkErrorOnlyWhenTheTestSaysSo() async {
    let transport = MockTransport(unanswered: .networkError)
    do {
      _ = try await transport.query(request: request)
      Issue.record("the call without a reply returned")
    } catch DbError.Network {
    } catch {
      Issue.record("wrong error \(error)")
    }
    #expect(transport.unscripted.isEmpty)
  }

  @Test
  func anUnknownTokenIsRecorded() {
    let transport = MockTransport()
    transport.unsubscribe(token: "never-issued")
    #expect(transport.unknownTokens == ["never-issued"])
  }
}
