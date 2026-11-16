import Foundation
import DBlayer

public struct Clip: DBModel {
    public static let modelName = "Clip"
    public let id: String
    public let title: String?
    public let sync: SyncInfo
    public let uploads: [String: UploadInfo]
    public let rowRevision: Int64
    public let estimatedBytes: UInt64

    public init(id: String, title: String? = nil, sync: SyncInfo = SyncInfo(), uploads: [String: UploadInfo] = [:], rowRevision: Int64 = 0, estimatedBytes: UInt64 = 0) {
        self.id = id
        self.title = title
        self.sync = sync
        self.uploads = uploads
        self.rowRevision = rowRevision
        self.estimatedBytes = estimatedBytes
    }

    public static let wireLayout = "e9226eea266088346d0e13a891650ac9945faf4f4ff01bfa2daed4a6ed5e8092"

    public init(wire row: WireRow) throws {
        let r = CellReader(row.cells)
        self.id = row.id
        self.title = try r.opt(0, \.asText)
        self.sync = SyncInfo(wire: row.sync)
        self.uploads = Dictionary(uniqueKeysWithValues: row.uploads.map { ($0.action, UploadInfo(wire: $0.upload)) })
        self.rowRevision = row.revision
        self.estimatedBytes = row.estimatedBytes
    }

    public static let title = Column<Clip, String?>("title")
    public static func `where`(_ condition: Condition) -> Query<Self> { Query<Self>().where(condition) }
    public static func createFileVariables(widgets: [StickerWidgetInput]? = nil) throws -> [String: JSONValue] {
        var variables_: [String: JSONValue] = [:]
        if let value = widgets { variables_["widgets"] = try JSONValue.encode(value) }
        return variables_
    }
    nonisolated(nonsending) public static func create(title: String? = nil, in scope_: AccountScope? = nil) async throws -> MutationReceipt<Self> {
        var values_: [String: JSONValue] = [:]
        if let value = title { values_["title"] = try JSONValue.encode(value) }
        return try await Self.create(values: values_, in: scope_)
    }
    nonisolated(nonsending) public func update(title: String?? = nil, in scope_: AccountScope? = nil) async throws -> MutationReceipt<Self> {
        var values_: [String: JSONValue] = [:]
        if let value = title { values_["title"] = try JSONValue.encode(value) }
        return try await self.update(values: values_, in: scope_)
    }
    nonisolated(nonsending) public static func createFile(id: String, source: String, widgets: [StickerWidgetInput]? = nil, prepare: Prepare? = nil, maxBytes: UInt64? = nil, in scope_: AccountScope) async throws -> MutationReceipt<Self> {
        var variables_: [String: JSONValue] = [:]
        if let value = widgets { variables_["widgets"] = try JSONValue.encode(value) }
        return try await scope_.upload(Self.self, id: id, action: "create_file", UploadRequest(path: source, variables: variables_, prepare: prepare, maxBytes: maxBytes))
    }
    nonisolated(nonsending) public func createFile(_ request: UploadRequest, in scope_: AccountScope) async throws -> MutationReceipt<Self> {
        try await scope_.upload(Self.self, id: id, action: "create_file", request)
    }
    nonisolated(nonsending) public static func stageCreateFile(source: String, prepare: Prepare? = nil, maxBytes: UInt64? = nil, in scope_: AccountScope) async throws -> StagedUpload<Self> {
        try await scope_.stage(Self.self, action: "create_file", UploadRequest(path: source, prepare: prepare, maxBytes: maxBytes))
    }
}

public struct ClipCreateFileResult: Codable, Sendable {
}

public struct Inbox: DBModel {
    public static let modelName = "Inbox"
    public let id: String
    public let title: String
    public let stance: String?
    public let sync: SyncInfo
    public let uploads: [String: UploadInfo]
    public let rowRevision: Int64
    public let estimatedBytes: UInt64

    public init(id: String, title: String, stance: String? = nil, sync: SyncInfo = SyncInfo(), uploads: [String: UploadInfo] = [:], rowRevision: Int64 = 0, estimatedBytes: UInt64 = 0) {
        self.id = id
        self.title = title
        self.stance = stance
        self.sync = sync
        self.uploads = uploads
        self.rowRevision = rowRevision
        self.estimatedBytes = estimatedBytes
    }

    public static let wireLayout = "2069e3ab4783e460fc6cb6b14859d746510bc8544c6342cb14c9ff5a89f93b0c"

    public init(wire row: WireRow) throws {
        let r = CellReader(row.cells)
        self.id = row.id
        self.title = try r.req(0, \.asText)
        self.stance = try r.opt(1, \.asText)
        self.sync = SyncInfo(wire: row.sync)
        self.uploads = Dictionary(uniqueKeysWithValues: row.uploads.map { ($0.action, UploadInfo(wire: $0.upload)) })
        self.rowRevision = row.revision
        self.estimatedBytes = row.estimatedBytes
    }

    public static let title = Column<Inbox, String>("title")
    public static let stance = Column<Inbox, String?>("stance")
    public static func `where`(_ condition: Condition) -> Query<Self> { Query<Self>().where(condition) }
    public static func shelf(list: ContainerShelfKind) throws -> Query<Self> {
        let built_ = Query<Self>(relation: "shelf", params: ["list": try JSONValue.encode(list)])
        return built_
    }
    nonisolated(nonsending) public static func create(title: String, stance: String? = nil, in scope_: AccountScope? = nil) async throws -> MutationReceipt<Self> {
        var values_: [String: JSONValue] = [:]
        values_["title"] = try JSONValue.encode(title)
        if let value = stance { values_["stance"] = try JSONValue.encode(value) }
        return try await Self.create(values: values_, in: scope_)
    }
    nonisolated(nonsending) public func update(title: String? = nil, stance: String?? = nil, in scope_: AccountScope? = nil) async throws -> MutationReceipt<Self> {
        var values_: [String: JSONValue] = [:]
        if let value = title { values_["title"] = try JSONValue.encode(value) }
        if let value = stance { values_["stance"] = try JSONValue.encode(value) }
        return try await self.update(values: values_, in: scope_)
    }
    nonisolated(nonsending) public func move(list: ContainerShelfKind, in scope_: AccountScope? = nil) async throws -> MutationReceipt<Self> {
        let values_ = try JSONValue.readObject(#"{"stance":{"$map":{"from":"list","map":{"PRIMARY":"main","SECONDARY":"other"}}}}"#)
        return try await self.act("move", values: values_, args: ["list": try JSONValue.encode(list)], in: scope_)
    }
}

public struct MediaEntry: DBModel {
    public static let modelName = "MediaEntry"
    public let id: String
    public let text: String?
    public let sequenceNumber: Int
    public let sync: SyncInfo
    public let uploads: [String: UploadInfo]
    public let rowRevision: Int64
    public let estimatedBytes: UInt64

    public init(id: String, text: String? = nil, sequenceNumber: Int, sync: SyncInfo = SyncInfo(), uploads: [String: UploadInfo] = [:], rowRevision: Int64 = 0, estimatedBytes: UInt64 = 0) {
        self.id = id
        self.text = text
        self.sequenceNumber = sequenceNumber
        self.sync = sync
        self.uploads = uploads
        self.rowRevision = rowRevision
        self.estimatedBytes = estimatedBytes
    }

    public static let wireLayout = "3d888c449b3a4c344c639a31a082e38e5a5856b16d4b932bfd615e243785441d"

    public init(wire row: WireRow) throws {
        let r = CellReader(row.cells)
        self.id = row.id
        self.text = try r.opt(0, \.asText)
        self.sequenceNumber = try r.req(1, \.asInt)
        self.sync = SyncInfo(wire: row.sync)
        self.uploads = Dictionary(uniqueKeysWithValues: row.uploads.map { ($0.action, UploadInfo(wire: $0.upload)) })
        self.rowRevision = row.revision
        self.estimatedBytes = row.estimatedBytes
    }

    public static let text = Column<MediaEntry, String?>("text")
    public static let sequenceNumber = Column<MediaEntry, Int>("sequenceNumber")
    public static func `where`(_ condition: Condition) -> Query<Self> { Query<Self>().where(condition) }
    public static func media(_ chatId: String) throws -> Query<Self> {
        let built_ = Query<Self>(relation: "media", params: ["chatId": try JSONValue.encode(chatId)])
        return built_
    }
    nonisolated(nonsending) public static func create(text: String? = nil, sequenceNumber: Int, in scope_: AccountScope? = nil) async throws -> MutationReceipt<Self> {
        var values_: [String: JSONValue] = [:]
        if let value = text { values_["text"] = try JSONValue.encode(value) }
        values_["sequenceNumber"] = try JSONValue.encode(sequenceNumber)
        return try await Self.create(values: values_, in: scope_)
    }
    nonisolated(nonsending) public func update(text: String?? = nil, sequenceNumber: Int? = nil, in scope_: AccountScope? = nil) async throws -> MutationReceipt<Self> {
        var values_: [String: JSONValue] = [:]
        if let value = text { values_["text"] = try JSONValue.encode(value) }
        if let value = sequenceNumber { values_["sequenceNumber"] = try JSONValue.encode(value) }
        return try await self.update(values: values_, in: scope_)
    }
}

public struct Roster: DBModel {
    public static let modelName = "Roster"
    public let id: String
    public let title: String
    public let sync: SyncInfo
    public let uploads: [String: UploadInfo]
    public let rowRevision: Int64
    public let estimatedBytes: UInt64

    public init(id: String, title: String, sync: SyncInfo = SyncInfo(), uploads: [String: UploadInfo] = [:], rowRevision: Int64 = 0, estimatedBytes: UInt64 = 0) {
        self.id = id
        self.title = title
        self.sync = sync
        self.uploads = uploads
        self.rowRevision = rowRevision
        self.estimatedBytes = estimatedBytes
    }

    public static let wireLayout = "e9226eea266088346d0e13a891650ac9945faf4f4ff01bfa2daed4a6ed5e8092"

    public init(wire row: WireRow) throws {
        let r = CellReader(row.cells)
        self.id = row.id
        self.title = try r.req(0, \.asText)
        self.sync = SyncInfo(wire: row.sync)
        self.uploads = Dictionary(uniqueKeysWithValues: row.uploads.map { ($0.action, UploadInfo(wire: $0.upload)) })
        self.rowRevision = row.revision
        self.estimatedBytes = row.estimatedBytes
    }

    public static let title = Column<Roster, String>("title")
    public static func `where`(_ condition: Condition) -> Query<Self> { Query<Self>().where(condition) }
    public static func containerMembers(_ containerId: String, order: ContainerMemberOrder = .joined) throws -> Query<Self> {
        let built_ = Query<Self>(relation: "container_members", params: ["containerId": try JSONValue.encode(containerId), "order": try JSONValue.encode(order)])
        return built_
    }
    public static func completeMembers(_ containerId: String, order: ContainerMemberOrder = .joined) throws -> Query<Self> {
        let built_ = Query<Self>(relation: "complete_members", params: ["containerId": try JSONValue.encode(containerId), "order": try JSONValue.encode(order)])
        return built_
    }
    nonisolated(nonsending) public static func create(title: String, in scope_: AccountScope? = nil) async throws -> MutationReceipt<Self> {
        var values_: [String: JSONValue] = [:]
        values_["title"] = try JSONValue.encode(title)
        return try await Self.create(values: values_, in: scope_)
    }
    nonisolated(nonsending) public func update(title: String? = nil, in scope_: AccountScope? = nil) async throws -> MutationReceipt<Self> {
        var values_: [String: JSONValue] = [:]
        if let value = title { values_["title"] = try JSONValue.encode(value) }
        return try await self.update(values: values_, in: scope_)
    }
}

public enum ContainerShelfKind: Codable, Sendable, Hashable {
    case primary
    case secondary
    case unknown(String)

    public init(rawValue: String) {
        switch rawValue {
        case "PRIMARY": self = .primary
        case "SECONDARY": self = .secondary
        default: self = .unknown(rawValue)
        }
    }

    public var rawValue: String {
        switch self {
        case .primary: return "PRIMARY"
        case .secondary: return "SECONDARY"
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

public enum ContainerMemberOrder: Codable, Sendable, Hashable {
    case joined
    case ownerFirst
    case unknown(String)

    public init(rawValue: String) {
        switch rawValue {
        case "JOINED": self = .joined
        case "OWNER_FIRST": self = .ownerFirst
        default: self = .unknown(rawValue)
        }
    }

    public var rawValue: String {
        switch self {
        case .joined: return "JOINED"
        case .ownerFirst: return "OWNER_FIRST"
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

public struct StickerWidgetInput: Encodable, Sendable, Equatable {
    public let kind: WidgetKind
    public let props: StickerPropsInput
    public let extras: GraphQLNullable<[StickerPropsInput]>

    public init(kind: WidgetKind, props: StickerPropsInput, extras: GraphQLNullable<[StickerPropsInput]> = .omitted) {
        self.kind = kind
        self.props = props
        self.extras = extras
    }

    enum CodingKeys: String, CodingKey {
        case kind
        case props
        case extras
    }

    public func encode(to encoder: any Encoder) throws {
        var container = encoder.container(keyedBy: CodingKeys.self)
        try container.encode(self.kind, forKey: .kind)
        try container.encode(self.props, forKey: .props)
        try container.encode(self.extras, forKey: .extras)
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
    public let recursive: GraphQLNullable<RecursiveInput>
    public let keywords: GraphQLNullable<KeywordInput>

    public init(name: String, mood: GraphQLNullable<Mood> = .omitted, tags: [String], notes: GraphQLNullable<[String?]> = .omitted, size: GraphQLNullable<Int> = .omitted, pinned: Bool, owner: OwnerInput, helpers: GraphQLNullable<[OwnerInput]> = .omitted, recursive: GraphQLNullable<RecursiveInput> = .omitted, keywords: GraphQLNullable<KeywordInput> = .omitted) {
        self.name = name
        self.mood = mood
        self.tags = tags
        self.notes = notes
        self.size = size
        self.pinned = pinned
        self.owner = owner
        self.helpers = helpers
        self.recursive = recursive
        self.keywords = keywords
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
        case recursive
        case keywords
    }

    public func encode(to encoder: any Encoder) throws {
        var container = encoder.container(keyedBy: CodingKeys.self)
        try container.encode(self.name, forKey: .name)
        try container.encode(self.mood, forKey: .mood)
        try container.encode(self.tags, forKey: .tags)
        try container.encode(self.notes, forKey: .notes)
        try container.encode(self.size, forKey: .size)
        try container.encode(self.pinned, forKey: .pinned)
        try container.encode(self.owner, forKey: .owner)
        try container.encode(self.helpers, forKey: .helpers)
        try container.encode(self.recursive, forKey: .recursive)
        try container.encode(self.keywords, forKey: .keywords)
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
        try container.encode(self.file, forKey: .file)
        try container.encode(self.widgets, forKey: .widgets)
    }
}

public indirect enum StickerPropsInput: Encodable, Sendable, Equatable {
    case text(StickerTextInput)
    case emoji(StickerEmojiInput)
    case tags([String])

    enum CodingKeys: String, CodingKey {
        case text
        case emoji
        case tags
    }

    public func encode(to encoder: any Encoder) throws {
        var container = encoder.container(keyedBy: CodingKeys.self)
        switch self {
        case .text(let value): try container.encode(value, forKey: .text)
        case .emoji(let value): try container.encode(value, forKey: .emoji)
        case .tags(let value): try container.encode(value, forKey: .tags)
        }
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
        try container.encode(self.id, forKey: .id)
        try container.encode(self.nickname, forKey: .nickname)
    }
}

public struct RecursiveInput: Encodable, Sendable, Equatable {
    public let next: GraphQLNullable<RecursiveInput>
    public let choice: GraphQLNullable<RecursiveChoice>

    public init(next: GraphQLNullable<RecursiveInput> = .omitted, choice: GraphQLNullable<RecursiveChoice> = .omitted) {
        self.next = next
        self.choice = choice
    }

    enum CodingKeys: String, CodingKey {
        case next
        case choice
    }

    public func encode(to encoder: any Encoder) throws {
        var container = encoder.container(keyedBy: CodingKeys.self)
        try container.encode(self.next, forKey: .next)
        try container.encode(self.choice, forKey: .choice)
    }
}

public struct KeywordInput: Encodable, Sendable, Equatable {
    public let `default`: String
    public let when: String
    public let `class`: String
    public let container: String
    public let encoder: String
    public let choices: [KeywordChoice]

    public init(`default`: String, when: String, `class`: String, container: String, encoder: String, choices: [KeywordChoice]) {
        self.`default` = `default`
        self.when = when
        self.`class` = `class`
        self.container = container
        self.encoder = encoder
        self.choices = choices
    }

    enum CodingKeys: String, CodingKey {
        case `default`
        case when
        case `class`
        case container
        case encoder
        case choices
    }

    public func encode(to encoder: any Encoder) throws {
        var container = encoder.container(keyedBy: CodingKeys.self)
        try container.encode(self.`default`, forKey: .`default`)
        try container.encode(self.when, forKey: .when)
        try container.encode(self.`class`, forKey: .`class`)
        try container.encode(self.container, forKey: .container)
        try container.encode(self.encoder, forKey: .encoder)
        try container.encode(self.choices, forKey: .choices)
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
        try container.encode(self.kind, forKey: .kind)
        try container.encode(self.settings, forKey: .settings)
    }
}

public struct StickerTextInput: Encodable, Sendable, Equatable {
    public let body: String
    public let sizes: GraphQLNullable<[Int]>

    public init(body: String, sizes: GraphQLNullable<[Int]> = .omitted) {
        self.body = body
        self.sizes = sizes
    }

    enum CodingKeys: String, CodingKey {
        case body
        case sizes
    }

    public func encode(to encoder: any Encoder) throws {
        var container = encoder.container(keyedBy: CodingKeys.self)
        try container.encode(self.body, forKey: .body)
        try container.encode(self.sizes, forKey: .sizes)
    }
}

public struct StickerEmojiInput: Encodable, Sendable, Equatable {
    public let code: String
    public let kind: GraphQLNullable<WidgetKind>

    public init(code: String, kind: GraphQLNullable<WidgetKind> = .omitted) {
        self.code = code
        self.kind = kind
    }

    enum CodingKeys: String, CodingKey {
        case code
        case kind
    }

    public func encode(to encoder: any Encoder) throws {
        var container = encoder.container(keyedBy: CodingKeys.self)
        try container.encode(self.code, forKey: .code)
        try container.encode(self.kind, forKey: .kind)
    }
}

public indirect enum RecursiveChoice: Encodable, Sendable, Equatable {
    case next(RecursiveChoice)
    case value(String)

    enum CodingKeys: String, CodingKey {
        case next
        case value
    }

    public func encode(to encoder: any Encoder) throws {
        var container = encoder.container(keyedBy: CodingKeys.self)
        switch self {
        case .next(let value): try container.encode(value, forKey: .next)
        case .value(let value): try container.encode(value, forKey: .value)
        }
    }
}

public indirect enum KeywordChoice: Encodable, Sendable, Equatable {
    case `default`(String)
    case when(String)

    enum CodingKeys: String, CodingKey {
        case `default`
        case when
    }

    public func encode(to encoder: any Encoder) throws {
        var container = encoder.container(keyedBy: CodingKeys.self)
        switch self {
        case .`default`(let value): try container.encode(value, forKey: .`default`)
        case .when(let value): try container.encode(value, forKey: .when)
        }
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
        try container.encode(self.itemProps, forKey: .itemProps)
        try container.encode(self.rotate, forKey: .rotate)
    }
}

public struct WhenResult: Codable, Sendable, Equatable {
    public let `default`: String?
    public let when: String?
    public let container: String?
    public let r: String?

    public init(`default`: String? = nil, when: String? = nil, container: String? = nil, r: String? = nil) {
        self.`default` = `default`
        self.when = when
        self.container = container
        self.r = r
    }

    private enum CodingKeys: String, CodingKey {
        case `default` = "default"
        case when = "when"
        case container = "container"
        case r = "r"
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
}

public struct StickerSendResult: Codable, Sendable, Equatable {
    public let id: String?

    public init(id: String? = nil) {
        self.id = id
    }

    private enum CodingKeys: String, CodingKey {
        case id = "id"
    }
}

public struct PricingLoadResult: Codable, Sendable, Equatable {
    public let rewards: [PricingLoadResultRewards]?
    public let videoIntro: PricingLoadResultVideoIntro?
    public let tags: [String]?
    public let spec: JSONValue?

    public init(rewards: [PricingLoadResultRewards]? = nil, videoIntro: PricingLoadResultVideoIntro? = nil, tags: [String]? = nil, spec: JSONValue? = nil) {
        self.rewards = rewards
        self.videoIntro = videoIntro
        self.tags = tags
        self.spec = spec
    }

    private enum CodingKeys: String, CodingKey {
        case rewards = "rewards"
        case videoIntro = "videoIntro"
        case tags = "tags"
        case spec = "spec"
    }
}

public struct PricingLoadResultRewards: Codable, Sendable, Equatable {
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
}

public struct PricingLoadResultVideoIntro: Codable, Sendable, Equatable {
    public let enabled: Bool?

    public init(enabled: Bool? = nil) {
        self.enabled = enabled
    }

    private enum CodingKeys: String, CodingKey {
        case enabled = "enabled"
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
}

public struct MomentCreateResultMoment: Codable, Sendable, Equatable {
    public let id: String?

    public init(id: String? = nil) {
        self.id = id
    }

    private enum CodingKeys: String, CodingKey {
        case id = "id"
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
}

public struct WidgetsSendResult: Codable, Sendable, Equatable {
    public let id: String?

    public init(id: String? = nil) {
        self.id = id
    }

    private enum CodingKeys: String, CodingKey {
        case id = "id"
    }
}

public struct ContainerMediaLoadResult: Codable, Sendable, Equatable {
    public let visualCount: Int?
    public let audioCount: Int?
    public let rewardCount: Int?
    public let latestSequence: Int?

    public init(visualCount: Int? = nil, audioCount: Int? = nil, rewardCount: Int? = nil, latestSequence: Int? = nil) {
        self.visualCount = visualCount
        self.audioCount = audioCount
        self.rewardCount = rewardCount
        self.latestSequence = latestSequence
    }

    private enum CodingKeys: String, CodingKey {
        case visualCount = "visualCount"
        case audioCount = "audioCount"
        case rewardCount = "rewardCount"
        case latestSequence = "latestSequence"
    }
}

public struct PeerLoadResult: Codable, Sendable, Equatable {
    public let peer: PeerLoadResultPeer?

    public init(peer: PeerLoadResultPeer? = nil) {
        self.peer = peer
    }

    private enum CodingKeys: String, CodingKey {
        case peer = "peer"
    }
}

public enum PeerLoadResultPeer: Codable, Sendable, Equatable {
    case person(PeerLoadResultPeerPerson)
    case robot(PeerLoadResultPeerRobot)
    case unknown(typename: String)

    public init(from decoder: Decoder) throws {
        let typename = try decoder.container(keyedBy: UnionTypenameKey.self).decode(String.self, forKey: .typename)
        switch typename {
        case "Person": self = .person(try PeerLoadResultPeerPerson(from: decoder))
        case "Robot": self = .robot(try PeerLoadResultPeerRobot(from: decoder))
        default: self = .unknown(typename: typename)
        }
    }

    public func encode(to encoder: Encoder) throws {
        switch self {
        case let .person(value): try JSONValue.tagged("Person", value).encode(to: encoder)
        case let .robot(value): try JSONValue.tagged("Robot", value).encode(to: encoder)
        case let .unknown(typename): try JSONValue.tagged(typename, [String: String]()).encode(to: encoder)
        }
    }

    public var kind: PeerLoadResultPeerKind? {
        switch self {
        case .person: return .person
        case .robot: return .robot
        case .unknown: return nil
        }
    }
}

public enum PeerLoadResultPeerKind: String, Codable, Sendable {
    case person = "Person"
    case robot = "Robot"
}

public struct PeerLoadResultPeerPerson: Codable, Sendable, Equatable {
    public let name: String?

    public init(name: String? = nil) {
        self.name = name
    }

    private enum CodingKeys: String, CodingKey {
        case name = "name"
    }
}

public struct PeerLoadResultPeerRobot: Codable, Sendable, Equatable {
    public let serial: String?

    public init(serial: String? = nil) {
        self.serial = serial
    }

    private enum CodingKeys: String, CodingKey {
        case serial = "serial"
    }
}

public struct ChatLibraryResult: Codable, Sendable, Equatable {
    public let mediaLibrary: ChatLibraryResultMediaLibrary?

    public init(mediaLibrary: ChatLibraryResultMediaLibrary? = nil) {
        self.mediaLibrary = mediaLibrary
    }

    private enum CodingKeys: String, CodingKey {
        case mediaLibrary = "mediaLibrary"
    }
}

public struct ChatLibraryResultMediaLibrary: Codable, Sendable, Equatable {
    public let visualCount: Int?
    public let audioCount: Int?

    public init(visualCount: Int? = nil, audioCount: Int? = nil) {
        self.visualCount = visualCount
        self.audioCount = audioCount
    }

    private enum CodingKeys: String, CodingKey {
        case visualCount = "visualCount"
        case audioCount = "audioCount"
    }
}

public enum Operations {
    nonisolated(nonsending) public static func when(`class`: String, in scope_: AccountScope? = nil) async throws -> WhenResult {
        var variables_: [String: JSONValue] = [:]
        variables_["class"] = try JSONValue.encode(`class`)
        return try await DB.call("KeywordCall", variables: variables_, as: WhenResult.self, in: scope_)
    }
    nonisolated(nonsending) public static func qrCodeCreate(kind: String, size: Int? = nil, in scope_: AccountScope? = nil) async throws -> QrCodeCreateResult {
        var variables_: [String: JSONValue] = [:]
        variables_["kind"] = try JSONValue.encode(kind)
        if let value = size { variables_["size"] = try JSONValue.encode(value) }
        return try await DB.call("QrCodeCreate", variables: ["input": JSONValue.object(variables_)], as: QrCodeCreateResult.self, in: scope_)
    }
    nonisolated(nonsending) public static func signOut(in scope_: AccountScope? = nil) async throws {
        let variables_: [String: JSONValue] = [:]
        try await DB.call("SignOut", variables: variables_, in: scope_)
    }
    nonisolated(nonsending) public static func stickerSend(input: StickerInput, in scope_: AccountScope? = nil) async throws -> StickerSendResult {
        var variables_: [String: JSONValue] = [:]
        variables_["input"] = try JSONValue.encode(input)
        return try await DB.call("StickerSend", variables: variables_, as: StickerSendResult.self, in: scope_)
    }
    nonisolated(nonsending) public static func pricingLoad(in scope_: AccountScope? = nil) async throws -> PricingLoadResult {
        let variables_: [String: JSONValue] = [:]
        return try await DB.call("Pricing", variables: variables_, as: PricingLoadResult.self, in: scope_)
    }
    nonisolated(nonsending) public static func momentCreate(input: MomentInput, in scope_: AccountScope? = nil) async throws -> MomentCreateResult {
        var variables_: [String: JSONValue] = [:]
        variables_["input"] = try JSONValue.encode(input)
        return try await DB.call("MomentCreate", variables: variables_, as: MomentCreateResult.self, in: scope_)
    }
    nonisolated(nonsending) public static func giphyLoad(type: String, limit: Int? = nil, in scope_: AccountScope? = nil) async throws -> [GiphyLoadResult] {
        var variables_: [String: JSONValue] = [:]
        variables_["type"] = try JSONValue.encode(type)
        if let value = limit { variables_["limit"] = try JSONValue.encode(value) }
        return try await DB.call("Giphy", variables: variables_, as: [GiphyLoadResult].self, in: scope_)
    }
    nonisolated(nonsending) public static func widgetsSend(widgets: [StickerWidgetInput]? = nil, in scope_: AccountScope? = nil) async throws -> WidgetsSendResult {
        var variables_: [String: JSONValue] = [:]
        if let value = widgets { variables_["widgets"] = try JSONValue.encode(value) }
        return try await DB.call("WidgetsSend", variables: variables_, as: WidgetsSendResult.self, in: scope_)
    }
    nonisolated(nonsending) public static func containerMediaLoad(containerId: String, upToSequence: Int? = nil, in scope_: AccountScope? = nil) async throws -> ContainerMediaLoadResult {
        var variables_: [String: JSONValue] = [:]
        variables_["containerId"] = try JSONValue.encode(containerId)
        if let value = upToSequence { variables_["upToSequence"] = try JSONValue.encode(value) }
        return try await DB.call("containerMedia", variables: variables_, as: ContainerMediaLoadResult.self, in: scope_)
    }
    nonisolated(nonsending) public static func peerLoad(in scope_: AccountScope? = nil) async throws -> PeerLoadResult {
        let variables_: [String: JSONValue] = [:]
        return try await DB.call("PeerLoad", variables: variables_, as: PeerLoadResult.self, in: scope_)
    }
    nonisolated(nonsending) public static func chatLibrary(id: String, in scope_: AccountScope? = nil) async throws -> ChatLibraryResult {
        var variables_: [String: JSONValue] = [:]
        variables_["id"] = try JSONValue.encode(id)
        return try await DB.call("ChatLibrary", variables: variables_, as: ChatLibraryResult.self, in: scope_)
    }
}
