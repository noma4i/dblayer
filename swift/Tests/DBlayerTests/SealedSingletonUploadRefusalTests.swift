import DBlayerTesting
import Foundation
import Testing
@testable import DBlayer

extension BridgeProbeTests {
  @MainActor @Test(.timeLimit(.minutes(1)), arguments: [false, true])
  func sealedSingletonUploadRefusalDiscardAllowsAnotherUpload(dataPresent: Bool) async throws {
    try await sealedSingletonUploadRefusal(dataPresent: dataPresent, discard: true)
  }

  @MainActor @Test(.timeLimit(.minutes(1)), arguments: [false, true])
  func sealedSingletonUploadRefusalIsReplaced(dataPresent: Bool) async throws {
    try await sealedSingletonUploadRefusal(dataPresent: dataPresent, discard: false)
  }

  @MainActor
  private func sealedSingletonUploadRefusal(dataPresent: Bool, discard: Bool) async throws {
    let first = FileManager.default.temporaryDirectory.appending(path: UUID().uuidString)
    let second = FileManager.default.temporaryDirectory.appending(path: UUID().uuidString)
    try Data("first".utf8).write(to: first)
    try Data("second".utf8).write(to: second)
    defer { try? FileManager.default.removeItem(at: first); try? FileManager.default.removeItem(at: second) }
    let transport = MockTransport()
    let definitions = #"[{"name":"CreateBlob","document":"mutation CreateBlob { createBlob { url headers signedId blobId } }"},{"name":"UserPhotoUpdate","document":"mutation UserPhotoUpdate { userPhotoUpdate { userPhoto { reason media { thumbUrl } } viewer { user { id name } } } }"}]"#
    _ = try await openSigned(config: Config(path: nil, dataVersion: nil), modelsJson: modelsJSON, transport: transport, operations: definitions)
    transport.enqueue("CreateBlob", reply: #"{"data":{"blobCreate":{"url":"https://storage.test/first","headers":{},"signedId":"first","blobId":"first"}}}"#)
    transport.enqueuePut("https://storage.test/first")
    let data = dataPresent ? #", "data":{"userPhotoUpdate":null}"# : ""
    let refusal = #"{"errors":[{"message":"no face","extensions":{"code":"FORBIDDEN","reasonCode":"no_face_detected"}}]"# + data + "}"
    let held = transport.hold("UserPhotoUpdate", reply: refusal)
    let receipt = try await CurrentUser.updatePhoto(source: first.path, in: DB.account())
    #expect(receipt.optimisticValue == nil)
    #expect(try await CurrentUser.ids(["singleton"]).read().isEmpty)
    try #require(await entered(held), "the final mutation did not reach the server")
    let queue = Cursor(try DB.account().operations())
    guard case let .queue(_, items)? = await queue.next("the first upload") else { throw WaitFailure.unexpectedOutcome }
    let info = try #require(items.first?.uploadInfo)
    let localURL = try #require(info.localUrl.flatMap(URL.init(string:)))
    #expect(FileManager.default.fileExists(atPath: localURL.path))
    let waiting = Task { try await receipt.confirmed(discardOnFailure: discard) }
    held.release()
    do {
      _ = try await valueBeforeDeadline(waiting, release: { held.release() })
      Issue.record("the refused upload was confirmed")
    } catch let DbError.Rejected(failure, kept) {
      #expect(failure.message == "no face")
      #expect(failure.extensions?.code == "FORBIDDEN")
      #expect(failure.extensions?.fields["reasonCode"] == "\"no_face_detected\"")
      #expect(kept == !discard)
    }
    let after = Cursor(try DB.account().operations())
    guard case let .queue(_, remaining)? = await after.next("the queue after the refusal") else { throw WaitFailure.unexpectedOutcome }
    #expect(remaining.isEmpty == discard, "requests: \(transport.calls.map(\.operationName))")
    transport.enqueue("CreateBlob", reply: #"{"data":{"blobCreate":{"url":"https://storage.test/second","headers":{},"signedId":"second","blobId":"second"}}}"#)
    transport.enqueuePut("https://storage.test/second")
    let next = transport.hold("UserPhotoUpdate", reply: #"{"data":{"userPhotoUpdate":{"userPhoto":{"reason":"ok","media":{"thumbUrl":"https://photo/2"}},"viewer":{"user":{"id":"u1","name":"Bea"}}}}}"#)
    let attempt = Task { try await CurrentUser.updatePhoto(source: second.path, in: DB.account()) }
    let sent = try await valueBeforeDeadline(attempt, release: { next.release() })
    try #require(await entered(next))
    let nextQueue = Cursor(try DB.account().operations())
    guard case let .queue(_, nextItems)? = await nextQueue.next("the replacement upload") else { throw WaitFailure.unexpectedOutcome }
    try #require(nextItems.count == 1)
    #expect(try #require(nextItems.first?.uploadInfo?.localUrl) != info.localUrl)
    #expect(!FileManager.default.fileExists(atPath: localURL.path))
    if !discard {
      do { _ = try await valueBeforeDeadline(Task { try await receipt.confirmed() }, release: { next.release() }); Issue.record("the replaced receipt did not end") }
      catch let DbError.Interrupted(detail) { #expect(detail == "Discarded") }
    }
    next.release()
    #expect(try await valueBeforeDeadline(Task { try await sent.confirmed() }, release: { next.release() }).value?.id == "u1")
    #expect(transport.unscripted.isEmpty, "requests: \(transport.calls.map(\.operationName))")
    await DB.closeForCleanup()
  }
}
