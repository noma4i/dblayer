import DBlayerTesting
import Foundation
import Testing
@testable import DBlayer

extension BridgeProbeTests {
  @MainActor @Test(.timeLimit(.minutes(1)), arguments: [false, true])
  func sealedUploadRefusalDiscardAllowsAnotherUpload(dataPresent: Bool) async throws {
    try await sealedUploadRefusal(dataPresent: dataPresent, discard: true)
  }

  @MainActor @Test(.timeLimit(.minutes(1)), arguments: [false, true])
  func sealedUploadRefusalIsReplaced(dataPresent: Bool) async throws {
    try await sealedUploadRefusal(dataPresent: dataPresent, discard: false)
  }

  @MainActor
  private func sealedUploadRefusal(dataPresent: Bool, discard: Bool) async throws {
    let first = FileManager.default.temporaryDirectory.appending(path: UUID().uuidString)
    let second = FileManager.default.temporaryDirectory.appending(path: UUID().uuidString)
    try Data("first".utf8).write(to: first)
    try Data("second".utf8).write(to: second)
    defer { try? FileManager.default.removeItem(at: first); try? FileManager.default.removeItem(at: second) }
    let transport = MockTransport()
    let definitions = #"[{"name":"CreateBlob","document":"mutation CreateBlob { createBlob { url headers signedId blobId } }"},{"name":"CreateMoment","document":"mutation CreateMoment { createMoment { id uuid likedByMe media createdAt successMessage } }"}]"#
    _ = try await openSigned(config: Config(path: nil, dataVersion: nil), modelsJson: modelsJSON, transport: transport, operations: definitions)
    transport.enqueue("CreateBlob", reply: #"{"data":{"blobCreate":{"url":"https://storage.test/first","headers":{},"signedId":"first","blobId":"first"}}}"#)
    transport.enqueuePut("https://storage.test/first")
    let data = dataPresent ? #", "data":{"createMoment":null}"# : ""
    let refusal = #"{"errors":[{"message":"no face","extensions":{"code":"FORBIDDEN","reasonCode":"no_face_detected"}}]"# + data + "}"
    let held = transport.hold("CreateMoment", reply: refusal)
    let receipt = try await Moment.uploadClip(id: "absent", source: first.path, in: DB.account())
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
    let next = transport.hold("CreateMoment", reply: #"{"data":{"createMoment":{"__typename":"Moment","id":"m77","uuid":"u","likedByMe":false,"media":{},"createdAt":"2026-10-08T00:00:00.000Z","successMessage":"second"}}}"#)
    let attempt = Task { try await Moment.uploadClip(id: "absent", source: second.path, in: DB.account()) }
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
    #expect(try await valueBeforeDeadline(Task { try await sent.confirmed() }, release: { next.release() }).value?.id == "m77")
    #expect(transport.unscripted.isEmpty, "requests: \(transport.calls.map(\.operationName))")
    await DB.closeForCleanup()
  }
}
