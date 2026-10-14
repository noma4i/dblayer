import Foundation
import DBlayer

public struct StickerInput: Encodable, Sendable {
    public let name: String
    public let mood: String?
    public let tags: [String]
    public let notes: [String?]?
    public let size: Int?
    public let pinned: Bool
    public let owner: OwnerInput
    public let helpers: [OwnerInput]?

    public init(name: String, mood: String? = nil, tags: [String], notes: [String?]? = nil, size: Int? = nil, pinned: Bool, owner: OwnerInput, helpers: [OwnerInput]? = nil) {
        self.name = name
        self.mood = mood
        self.tags = tags
        self.notes = notes
        self.size = size
        self.pinned = pinned
        self.owner = owner
        self.helpers = helpers
    }
}

public struct OwnerInput: Encodable, Sendable {
    public let id: String
    public let nickname: String?

    public init(id: String, nickname: String? = nil) {
        self.id = id
        self.nickname = nickname
    }
}

public struct QrCodeCreateResult: Codable, Sendable {
    public let url: String?
}

public struct StickerSendResult: Codable, Sendable {
    public let id: String?
}

public enum Operations {
    public static func qrCodeCreate(kind: String, size: Int? = nil) async throws -> QrCodeCreateResult {
        var variables: [String: JSONValue] = [:]
        variables["kind"] = JSONValue.encode(kind)
        if let value = size { variables["size"] = JSONValue.encode(value) }
        return try await DB.run("QrCodeCreate", variables: ["input": JSONValue.object(variables)], as: QrCodeCreateResult.self)
    }
    public static func signOut() async throws {
        var variables: [String: JSONValue] = [:]
        try await DB.run("SignOut", variables: variables)
    }
    public static func stickerSend(input: StickerInput) async throws -> StickerSendResult {
        var variables: [String: JSONValue] = [:]
        variables["input"] = JSONValue.encode(input)
        return try await DB.run("StickerSend", variables: variables, as: StickerSendResult.self)
    }
}
