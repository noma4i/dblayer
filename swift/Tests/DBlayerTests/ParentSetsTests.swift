import Foundation
import Testing
@testable import DBlayer

extension BridgeProbeTests {
struct ParentSetsTests {
  @Test
  func aLocalMessageMovesTheChatPointerAndDiscardingItGivesThePointerBack() async throws {
    DB.database = try Database.open(config: Config(path: nil, dataVersion: nil, cacheRows: 32), modelsJson: modelsJSON, transport: nil)
    let chat = try await Chat.create(title: "t")
    let message = try await Message.create(chatId: chat.id, authorId: "u1", text: "hi")

    let pointed = await Chat.find(chat.id).current()
    #expect(pointed?.lastMessageId == message.id)
    #expect(pointed?.lastMessageAt != nil)

    try await message.discard()
    let restored = await Chat.find(chat.id).current()
    #expect(restored?.lastMessageId == nil)
    #expect(restored?.lastMessageAt == nil)
  }
}
}
