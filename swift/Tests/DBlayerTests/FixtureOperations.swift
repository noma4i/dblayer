import Foundation
import DBlayer

public enum Mood: Codable, Sendable, Hashable {
    case happy
    case sad
    case unknown(String)

    public init(rawValue: String) {
        switch rawValue {
        case "HAPPY": self = .happy
        case "SAD": self = .sad
        default: self = .unknown(rawValue)
        }
    }

    public var rawValue: String {
        switch self {
        case .happy: return "HAPPY"
        case .sad: return "SAD"
        case .unknown(let raw): return raw
        }
    }

    public init(from decoder: Decoder) throws {
        self.init(rawValue: try decoder.singleValueContainer().decode(String.self))
    }

    public func encode(to encoder: Encoder) throws {
        var container = encoder.singleValueContainer()
        try container.encode(rawValue)
    }
}

public enum WidgetKind: Codable, Sendable, Hashable {
    case emoji
    case gif
    case unknown(String)

    public init(rawValue: String) {
        switch rawValue {
        case "EMOJI": self = .emoji
        case "GIF": self = .gif
        default: self = .unknown(rawValue)
        }
    }

    public var rawValue: String {
        switch self {
        case .emoji: return "EMOJI"
        case .gif: return "GIF"
        case .unknown(let raw): return raw
        }
    }

    public init(from decoder: Decoder) throws {
        self.init(rawValue: try decoder.singleValueContainer().decode(String.self))
    }

    public func encode(to encoder: Encoder) throws {
        var container = encoder.singleValueContainer()
        try container.encode(rawValue)
    }
}

public struct StickerInput: Encodable, Sendable, Equatable {
    public let name: String
    public let mood: GraphQLNullable<Mood>
    public let tags: [String]
    public let notes: GraphQLNullable<[String?]>
    public let size: GraphQLNullable<Int>
    public let pinned: Bool
    public let owner: OwnerInput
    public let helpers: GraphQLNullable<[OwnerInput]>

    public init(name: String, mood: GraphQLNullable<Mood> = .omitted, tags: [String], notes: GraphQLNullable<[String?]> = .omitted, size: GraphQLNullable<Int> = .omitted, pinned: Bool, owner: OwnerInput, helpers: GraphQLNullable<[OwnerInput]> = .omitted) {
        self.name = name
        self.mood = mood
        self.tags = tags
        self.notes = notes
        self.size = size
        self.pinned = pinned
        self.owner = owner
        self.helpers = helpers
    }

    enum CodingKeys: String, CodingKey {
        case name
        case mood
        case tags
        case notes
        case size
        case pinned
        case owner
        case helpers
    }

    public func encode(to encoder: any Encoder) throws {
        var container = encoder.container(keyedBy: CodingKeys.self)
        try container.encode(name, forKey: .name)
        try container.encode(mood, forKey: .mood)
        try container.encode(tags, forKey: .tags)
        try container.encode(notes, forKey: .notes)
        try container.encode(size, forKey: .size)
        try container.encode(pinned, forKey: .pinned)
        try container.encode(owner, forKey: .owner)
        try container.encode(helpers, forKey: .helpers)
    }
}

public struct MomentInput: Encodable, Sendable, Equatable {
    public let file: String
    public let widgets: GraphQLNullable<[WidgetInput]>

    public init(file: String, widgets: GraphQLNullable<[WidgetInput]> = .omitted) {
        self.file = file
        self.widgets = widgets
    }

    enum CodingKeys: String, CodingKey {
        case file
        case widgets
    }

    public func encode(to encoder: any Encoder) throws {
        var container = encoder.container(keyedBy: CodingKeys.self)
        try container.encode(file, forKey: .file)
        try container.encode(widgets, forKey: .widgets)
    }
}

public struct OwnerInput: Encodable, Sendable, Equatable {
    public let id: String
    public let nickname: GraphQLNullable<String>

    public init(id: String, nickname: GraphQLNullable<String> = .omitted) {
        self.id = id
        self.nickname = nickname
    }

    enum CodingKeys: String, CodingKey {
        case id
        case nickname
    }

    public func encode(to encoder: any Encoder) throws {
        var container = encoder.container(keyedBy: CodingKeys.self)
        try container.encode(id, forKey: .id)
        try container.encode(nickname, forKey: .nickname)
    }
}

public struct WidgetInput: Encodable, Sendable, Equatable {
    public let kind: WidgetKind
    public let settings: SettingsInput

    public init(kind: WidgetKind, settings: SettingsInput) {
        self.kind = kind
        self.settings = settings
    }

    enum CodingKeys: String, CodingKey {
        case kind
        case settings
    }

    public func encode(to encoder: any Encoder) throws {
        var container = encoder.container(keyedBy: CodingKeys.self)
        try container.encode(kind, forKey: .kind)
        try container.encode(settings, forKey: .settings)
    }
}

public struct SettingsInput: Encodable, Sendable, Equatable {
    public let itemProps: JSONValue
    public let rotate: GraphQLNullable<Double>

    public init(itemProps: JSONValue, rotate: GraphQLNullable<Double> = .omitted) {
        self.itemProps = itemProps
        self.rotate = rotate
    }

    enum CodingKeys: String, CodingKey {
        case itemProps
        case rotate
    }

    public func encode(to encoder: any Encoder) throws {
        var container = encoder.container(keyedBy: CodingKeys.self)
        try container.encode(itemProps, forKey: .itemProps)
        try container.encode(rotate, forKey: .rotate)
    }
}

public struct QrCodeCreateResult: Codable, Sendable, Equatable {
    public let url: String?

    public init(url: String? = nil) {
        self.url = url
    }

    private enum CodingKeys: String, CodingKey {
        case url = "url"
    }

    public init(from decoder: Decoder) throws {
        let container = try decoder.container(keyedBy: CodingKeys.self)
        url = try container.decodeIfPresent(String.self, forKey: .url)
    }
}

public struct StickerSendResult: Codable, Sendable, Equatable {
    public let id: String?

    public init(id: String? = nil) {
        self.id = id
    }

    private enum CodingKeys: String, CodingKey {
        case id = "id"
    }

    public init(from decoder: Decoder) throws {
        let container = try decoder.container(keyedBy: CodingKeys.self)
        id = try container.decodeIfPresent(String.self, forKey: .id)
    }
}

public struct PricingLoadResult: Codable, Sendable, Equatable {
    public let gifts: [PricingLoadResultGifts]?
    public let videoIntro: PricingLoadResultVideoIntro?
    public let tags: [String]?
    public let spec: JSONValue?

    public init(gifts: [PricingLoadResultGifts]? = nil, videoIntro: PricingLoadResultVideoIntro? = nil, tags: [String]? = nil, spec: JSONValue? = nil) {
        self.gifts = gifts
        self.videoIntro = videoIntro
        self.tags = tags
        self.spec = spec
    }

    private enum CodingKeys: String, CodingKey {
        case gifts = "gifts"
        case videoIntro = "videoIntro"
        case tags = "tags"
        case spec = "spec"
    }

    public init(from decoder: Decoder) throws {
        let container = try decoder.container(keyedBy: CodingKeys.self)
        gifts = try container.decodeIfPresent([PricingLoadResultGifts].self, forKey: .gifts)
        videoIntro = try container.decodeIfPresent(PricingLoadResultVideoIntro.self, forKey: .videoIntro)
        tags = try container.decodeIfPresent([String].self, forKey: .tags)
        spec = try container.decodeIfPresent(JSONValue.self, forKey: .spec)
    }
}

public struct PricingLoadResultGifts: Codable, Sendable, Equatable {
    public let id: String?
    public let name: String?
    public let price: Int?

    public init(id: String? = nil, name: String? = nil, price: Int? = nil) {
        self.id = id
        self.name = name
        self.price = price
    }

    private enum CodingKeys: String, CodingKey {
        case id = "id"
        case name = "name"
        case price = "price"
    }

    public init(from decoder: Decoder) throws {
        let container = try decoder.container(keyedBy: CodingKeys.self)
        id = try container.decodeIfPresent(IDText.self, forKey: .id)?.value
        name = try container.decodeIfPresent(String.self, forKey: .name)
        price = try container.decodeIfPresent(Int.self, forKey: .price)
    }
}

public struct PricingLoadResultVideoIntro: Codable, Sendable, Equatable {
    public let enabled: Bool?

    public init(enabled: Bool? = nil) {
        self.enabled = enabled
    }

    private enum CodingKeys: String, CodingKey {
        case enabled = "enabled"
    }

    public init(from decoder: Decoder) throws {
        let container = try decoder.container(keyedBy: CodingKeys.self)
        enabled = try container.decodeIfPresent(Bool.self, forKey: .enabled)
    }
}

public struct MomentCreateResult: Codable, Sendable, Equatable {
    public let moment: MomentCreateResultMoment?

    public init(moment: MomentCreateResultMoment? = nil) {
        self.moment = moment
    }

    private enum CodingKeys: String, CodingKey {
        case moment = "moment"
    }

    public init(from decoder: Decoder) throws {
        let container = try decoder.container(keyedBy: CodingKeys.self)
        moment = try container.decodeIfPresent(MomentCreateResultMoment.self, forKey: .moment)
    }
}

public struct MomentCreateResultMoment: Codable, Sendable, Equatable {
    public let id: String?

    public init(id: String? = nil) {
        self.id = id
    }

    private enum CodingKeys: String, CodingKey {
        case id = "id"
    }

    public init(from decoder: Decoder) throws {
        let container = try decoder.container(keyedBy: CodingKeys.self)
        id = try container.decodeIfPresent(IDText.self, forKey: .id)?.value
    }
}

public struct GiphyLoadResult: Codable, Sendable, Equatable {
    public let id: String?
    public let url: String?
    public let isSticker: Bool?

    public init(id: String? = nil, url: String? = nil, isSticker: Bool? = nil) {
        self.id = id
        self.url = url
        self.isSticker = isSticker
    }

    private enum CodingKeys: String, CodingKey {
        case id = "id"
        case url = "url"
        case isSticker = "isSticker"
    }

    public init(from decoder: Decoder) throws {
        let container = try decoder.container(keyedBy: CodingKeys.self)
        id = try container.decodeIfPresent(IDText.self, forKey: .id)?.value
        url = try container.decodeIfPresent(String.self, forKey: .url)
        isSticker = try container.decodeIfPresent(Bool.self, forKey: .isSticker)
    }
}

public enum Operations {
    public static func qrCodeCreate(kind: String, size: Int? = nil) async throws -> QrCodeCreateResult {
        var variables: [String: JSONValue] = [:]
        variables["kind"] = JSONValue.encode(kind)
        if let value = size { variables["size"] = JSONValue.encode(value) }
        return try await DB.run("QrCodeCreate", variables: ["input": JSONValue.object(variables)], as: QrCodeCreateResult.self)
    }
    public static func signOut() async throws {
        let variables: [String: JSONValue] = [:]
        try await DB.run("SignOut", variables: variables)
    }
    public static func stickerSend(input: StickerInput) async throws -> StickerSendResult {
        var variables: [String: JSONValue] = [:]
        variables["input"] = JSONValue.encode(input)
        return try await DB.run("StickerSend", variables: variables, as: StickerSendResult.self)
    }
    public static func pricingLoad() async throws -> PricingLoadResult {
        let variables: [String: JSONValue] = [:]
        return try await DB.run("Pricing", variables: variables, as: PricingLoadResult.self)
    }
    public static func momentCreate(input: MomentInput) async throws -> MomentCreateResult {
        var variables: [String: JSONValue] = [:]
        variables["input"] = JSONValue.encode(input)
        return try await DB.run("MomentCreate", variables: variables, as: MomentCreateResult.self)
    }
    public static func giphyLoad(type: String, limit: Int? = nil) async throws -> [GiphyLoadResult] {
        var variables: [String: JSONValue] = [:]
        variables["type"] = JSONValue.encode(type)
        if let value = limit { variables["limit"] = JSONValue.encode(value) }
        return try await DB.run("Giphy", variables: variables, as: [GiphyLoadResult].self)
    }
}
