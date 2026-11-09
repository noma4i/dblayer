import DBlayerTesting
import Foundation
import Testing
@testable import DBlayer

private func author(_ name: String) -> Author {
  switch name {
  case "outbox": return .outbox
  case "window": return .window
  default: fatalError("the contract table has no author \(name)")
  }
}

private func cases() throws -> [[String: Any]] {
  let root = URL(fileURLWithPath: #filePath).deletingLastPathComponent().deletingLastPathComponent().deletingLastPathComponent().deletingLastPathComponent()
  let data = try Data(contentsOf: root.appendingPathComponent("fixtures/mock-contract/pairs.json"))
  return try JSONSerialization.jsonObject(with: data) as! [[String: Any]]
}

private func text(_ value: Any) -> String {
  String(decoding: try! JSONSerialization.data(withJSONObject: value, options: [.sortedKeys, .fragmentsAllowed]), as: UTF8.self)
}

struct MockContractTests {
  /// Every platform runs this table against the one server contract of the core: a repeat of a pair gets the first reply, a second body under one pair and an unscripted request are refused.
  @Test(.timeLimit(.minutes(1)))
  func mock_contract_pairs_table() async throws {
    for entry in try cases() {
      let run = { () async in
        let mock = MockTransport()
        for (operation, replies) in entry["replies"] as! [String: [Any]] { for reply in replies { mock.enqueue(operation, reply: text(reply)) } }
        var seen: [String] = []
        for call in entry["calls"] as! [[Any]] {
          let request = TransportRequest(document: "mutation \(call[2]) { x }", operationName: call[2] as! String, variablesJson: text(call[3]), purpose: .standard, author: author(call[0] as! String), requestId: UInt64(call[1] as! Int))
          do { seen.append("data:\(try await mock.mutate(request: request))") } catch { seen.append("refused") }
        }
        #expect(seen == (entry["expect"] as! [String]), "\(entry["name"] as! String)")
      }
      if (entry["expect"] as! [String]).contains("refused") {
        await withKnownIssue { await run() } matching: { "\($0.comments)".contains("MockTransport") }
      } else {
        await run()
      }
    }
  }

  @Test
  func server_contract_object_pairs_table() throws {
    for entry in try cases() {
      let contract = ServerContract()
      for (operation, replies) in entry["replies"] as! [String: [Any]] { for reply in replies { contract.script(operation: operation, token: text(reply)) } }
      var seen: [String] = []
      for call in entry["calls"] as! [[Any]] {
        let wire = text(["requestId": "\(call[1])", "sequence": NSNull(), "body": ["operationName": call[2], "variables": call[3]]])
        let scope = call[0] as! String
        switch try contract.admit(scope: scope, requestJson: wire) {
        case .stored(let token): seen.append("data:\(token)")
        case .refused, .twoBodies: seen.append("refused")
        case .fresh:
          if let token = contract.take(operation: call[2] as! String) {
            try contract.settle(scope: scope, requestJson: wire, token: token)
            seen.append("data:\(token)")
          } else { seen.append("refused") }
        }
      }
      #expect(seen == (entry["expect"] as! [String]), "\(entry["name"] as! String)")
    }
  }

  /// A PUT nobody scripted is refused like a call without a reply.
  @Test(.timeLimit(.minutes(1)))
  func mock_contract_unscripted_put_is_refused() async {
    await withKnownIssue {
      let mock = MockTransport()
      let file = PutFile(path: "/nonexistent", offset: 0, length: 1)
      let identity = TransferIdentity(operationId: 1, itemId: "i", step: .part(number: 1))
      do {
        _ = try await mock.scriptedPut(url: "https://s3/unscripted", headers: [:], file: file, identity: identity, progress: ByteProgress(noHandle: .init()))
        Issue.record("the mock accepted a PUT nobody scripted")
      } catch {
        #expect("\(error)".contains("Invalid"))
      }
    } matching: { "\($0.comments)".contains("MockTransport") }
  }
}
