import DBlayerTesting
import Foundation
import Testing
@testable import DBlayer

/// The main-thread flag of every decoder call of the fixture models below.
private final class DecoderThreads: @unchecked Sendable {
  static let shared = DecoderThreads()
  private let lock = NSLock()
  private var flags: [Bool] = []
  func record() { lock.lock(); flags.append(Thread.isMainThread); lock.unlock() }
  func clear() { lock.lock(); flags = []; lock.unlock() }
  var calls: [Bool] { lock.lock(); defer { lock.unlock() }; return flags }
}

private struct ThreadChat: DBModel {
  static let modelName = Chat.modelName
  static let wireLayout = Chat.wireLayout
  let chat: Chat
  init(wire: WireRow) throws { DecoderThreads.shared.record(); chat = try Chat(wire: wire) }
  var id: String { chat.id }
  var sync: SyncInfo { chat.sync }
  var uploads: [String: UploadInfo] { chat.uploads }
  var rowRevision: Int64 { chat.rowRevision }
  var estimatedBytes: UInt64 { chat.estimatedBytes }
}

private struct ThreadNotice: DBModel {
  static let modelName = ChatActivityNotice.modelName
  static let wireLayout = ChatActivityNotice.wireLayout
  let notice: ChatActivityNotice
  init(wire: WireRow) throws { DecoderThreads.shared.record(); notice = try ChatActivityNotice(wire: wire) }
  var id: String { notice.id }
  var sync: SyncInfo { notice.sync }
  var uploads: [String: UploadInfo] { notice.uploads }
  var rowRevision: Int64 { notice.rowRevision }
  var estimatedBytes: UInt64 { notice.estimatedBytes }
}

private struct ThreadMoment: DBModel {
  static let modelName = Moment.modelName
  static let wireLayout = Moment.wireLayout
  let moment: Moment
  init(wire: WireRow) throws { DecoderThreads.shared.record(); moment = try Moment(wire: wire) }
  var id: String { moment.id }
  var sync: SyncInfo { moment.sync }
  var uploads: [String: UploadInfo] { moment.uploads }
  var rowRevision: Int64 { moment.rowRevision }
  var estimatedBytes: UInt64 { moment.estimatedBytes }
}

extension BridgeProbeTests {
  /// Requirement: the library decodes a model of the app off the main actor on every entry that hands a model to the caller: the rows of a stream, the events and the value of a write receipt. The reader on the main actor only receives the value. Observable: the main-thread flag the model decoder records, and the value the reader gets.
  @MainActor
  @Test(.timeLimit(.minutes(1)))
  func everyEntryDecodesTheModelOffTheMainActor() async throws {
    let transport = MockTransport()
    try await openSigned(config: Config(path: nil), modelsJson: modelsJSON, transport: transport, operations: #"[{"name":"ChatActionSignal","document":"subscription ChatActionSignal { chatActionSignal { chatId userId kind active } }"},{"name":"ChatList","document":"query ChatList { chats { id title revision lists } }"},{"name":"CreateMoment","document":"mutation CreateMoment($text: String) { createMoment(text: $text) { id uuid likedByMe media createdAt } }"}]"#)
    DecoderThreads.shared.clear()

    transport.enqueue("ChatList", reply: #"{"data":{"chats":[{"__typename":"Chat","id":"c1","title":"t","revision":1,"lists":["SECONDARY"]}]}}"#)
    let projected = Projected<ThreadChat>(try DB.account().stream(Query<ThreadChat>(relation: "list", params: ["list": .string("SECONDARY")])))
    try await Chat.list("SECONDARY").load()
    await eventually("the reader received the row") { projected.rows.map(\.id) == ["c1"] }
    #expect(!DecoderThreads.shared.calls.isEmpty, "Projected: the decoder never ran")
    #expect(!DecoderThreads.shared.calls.contains(true), "Projected: the decoder ran on the main thread: \(DecoderThreads.shared.calls)")

    DecoderThreads.shared.clear()
    let events = try DB.account().events(ThreadNotice.self)
    var received: String?
    for try await item in events {
      switch item {
      case .ready:
        await eventually("the signal subscription opens") { transport.calls.contains { $0.operationName == "ChatActionSignal" } }
        try transport.emit("ChatActionSignal", json: #"{"data":{"chatActionSignal":{"chatId":"c1","userId":"u","kind":"SCREENSHOT","active":true}}}"#)
      case .event(let event):
        received = event.after?.notice.chatId
      default: continue
      }
      if received != nil { break }
    }
    #expect(received == "c1")
    #expect(!DecoderThreads.shared.calls.isEmpty, "events: the decoder never ran")
    #expect(!DecoderThreads.shared.calls.contains(true), "events: the decoder ran on the main thread: \(DecoderThreads.shared.calls)")

    DecoderThreads.shared.clear()
    transport.enqueue("CreateMoment", reply: #"{"data":{"createMoment":{"__typename":"Moment","id":"m77","uuid":"u","likedByMe":false,"media":{},"createdAt":"2026-10-08T00:00:00.000Z"}}}"#)
    let sent = try await DB.account().send(ThreadMoment.self, values: ["text": .string("hello")])
    #expect(sent.optimisticValue != nil, "send: the receipt carries no optimistic value")
    #expect(!DecoderThreads.shared.calls.isEmpty, "send: the decoder never ran")
    #expect(!DecoderThreads.shared.calls.contains(true), "send: the decoder ran on the main thread: \(DecoderThreads.shared.calls)")
  }
}
