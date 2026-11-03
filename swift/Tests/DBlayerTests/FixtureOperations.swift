import Foundation
import DBlayer

public struct BlobProfile: DBSingleton {
    public static let modelName = "BlobProfile"
    public let id: String
    public let label: String?
    public let sync: SyncInfo
    public let uploads: [String: UploadInfo]

    public init(id: String, label: String? = nil, sync: SyncInfo = SyncInfo(), uploads: [String: UploadInfo] = [:]) {
        self.id = id
        self.label = label
        self.sync = sync
        self.uploads = uploads
    }

    public static let wireLayout = "cae70f3b4f7f7821a632570ad1a489bc597e5b848b1ccc6df664113173d98031"

    public init(wire row: WireRow) throws {
        let r = CellReader(row.cells)
        self.id = row.id
        self.label = try r.opt(0, \.asText)
        self.sync = SyncInfo(wire: row.sync)
        self.uploads = Dictionary(uniqueKeysWithValues: row.uploads.map { ($0.action, UploadInfo(wire: $0.upload)) })
    }

    public static let label = Column<BlobProfile, String?>("label")
    public static func `where`(_ condition: Condition) -> Query<Self> { Query<Self>().where(condition) }
    public static var current: Query<Self> {
        let built = Query<Self>(relation: "current", params: [:])
        return built
    }
    public static var singleton: Query<Self> { current }
    @concurrent public static func create(label: String? = nil, in scope: AccountScope? = nil) async throws -> Self {
        var values: [String: JSONValue] = [:]
        if let value = label { values["label"] = try JSONValue.encode(value) }
        return try await create(values: values, in: scope)
    }
    @concurrent public func update(label: String?? = nil, in scope: AccountScope? = nil) async throws -> Self {
        var values: [String: JSONValue] = [:]
        if let value = label { values["label"] = try JSONValue.encode(value) }
        return try await update(values: values, in: scope)
    }
    @concurrent public func setVideo(video: Blob, in scope: AccountScope? = nil) async throws -> Self {
        let values = try JSONValue.read(#"{}"#).objectValue
        return try await act("set_video", values: values, args: ["video": try JSONValue.encode(video)], in: scope)
    }
    @concurrent public static func setVideo(video: Blob, in scope: AccountScope? = nil) async throws -> Self {
        guard let current = try await singleton.read(in: scope).first else { throw DecodingError.valueNotFound(Self.self, .init(codingPath: [], debugDescription: "singleton is missing")) }
        return try await current.setVideo(video: video, in: scope)
    }
}

public struct Clip: DBModel {
    public static let modelName = "Clip"
    public let id: String
    public let title: String?
    public let sync: SyncInfo
    public let uploads: [String: UploadInfo]

    public init(id: String, title: String? = nil, sync: SyncInfo = SyncInfo(), uploads: [String: UploadInfo] = [:]) {
        self.id = id
        self.title = title
        self.sync = sync
        self.uploads = uploads
    }

    public static let wireLayout = "e9226eea266088346d0e13a891650ac9945faf4f4ff01bfa2daed4a6ed5e8092"

    public init(wire row: WireRow) throws {
        let r = CellReader(row.cells)
        self.id = row.id
        self.title = try r.opt(0, \.asText)
        self.sync = SyncInfo(wire: row.sync)
        self.uploads = Dictionary(uniqueKeysWithValues: row.uploads.map { ($0.action, UploadInfo(wire: $0.upload)) })
    }

    public static let title = Column<Clip, String?>("title")
    public static func `where`(_ condition: Condition) -> Query<Self> { Query<Self>().where(condition) }
    public static func createFileVariables(widgets: [StickerWidgetInput]? = nil) throws -> [String: JSONValue] {
        var variables: [String: JSONValue] = [:]
        if let value = widgets { variables["widgets"] = try JSONValue.encode(value) }
        return variables
    }
    @concurrent public static func create(title: String? = nil, in scope: AccountScope? = nil) async throws -> Self {
        var values: [String: JSONValue] = [:]
        if let value = title { values["title"] = try JSONValue.encode(value) }
        return try await create(values: values, in: scope)
    }
    @concurrent public func update(title: String?? = nil, in scope: AccountScope? = nil) async throws -> Self {
        var values: [String: JSONValue] = [:]
        if let value = title { values["title"] = try JSONValue.encode(value) }
        return try await update(values: values, in: scope)
    }
    @concurrent public func createFile(_ request: UploadRequest, in scope: AccountScope) async throws -> ActionAnswer<Self, ClipCreateFileResult> {
        try await scope.upload(self, action: "create_file", request)
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

    public init(id: String, title: String, stance: String? = nil, sync: SyncInfo = SyncInfo(), uploads: [String: UploadInfo] = [:]) {
        self.id = id
        self.title = title
        self.stance = stance
        self.sync = sync
        self.uploads = uploads
    }

    public static let wireLayout = "2069e3ab4783e460fc6cb6b14859d746510bc8544c6342cb14c9ff5a89f93b0c"

    public init(wire row: WireRow) throws {
        let r = CellReader(row.cells)
        self.id = row.id
        self.title = try r.req(0, \.asText)
        self.stance = try r.opt(1, \.asText)
        self.sync = SyncInfo(wire: row.sync)
        self.uploads = Dictionary(uniqueKeysWithValues: row.uploads.map { ($0.action, UploadInfo(wire: $0.upload)) })
    }

    public static let title = Column<Inbox, String>("title")
    public static let stance = Column<Inbox, String?>("stance")
    public static func `where`(_ condition: Condition) -> Query<Self> { Query<Self>().where(condition) }
    public static func shelf(list: ContainerShelfKind) throws -> Query<Self> {
        let built = Query<Self>(relation: "shelf", params: ["list": try JSONValue.encode(list)])
        return built
    }
    @concurrent public static func create(title: String, stance: String? = nil, in scope: AccountScope? = nil) async throws -> Self {
        var values: [String: JSONValue] = [:]
        values["title"] = try JSONValue.encode(title)
        if let value = stance { values["stance"] = try JSONValue.encode(value) }
        return try await create(values: values, in: scope)
    }
    @concurrent public func update(title: String? = nil, stance: String?? = nil, in scope: AccountScope? = nil) async throws -> Self {
        var values: [String: JSONValue] = [:]
        if let value = title { values["title"] = try JSONValue.encode(value) }
        if let value = stance { values["stance"] = try JSONValue.encode(value) }
        return try await update(values: values, in: scope)
    }
    @concurrent public func move(list: ContainerShelfKind, in scope: AccountScope? = nil) async throws -> Self {
        let values = try JSONValue.read(#"{"stance":{"$map":{"from":"list","map":{"PRIMARY":"main","SECONDARY":"other"}}}}"#).objectValue
        return try await act("move", values: values, args: ["list": try JSONValue.encode(list)], in: scope)
    }
}

public struct MediaEntry: DBModel {
    public static let modelName = "MediaEntry"
    public let id: String
    public let text: String?
    public let sequenceNumber: Int
    public let sync: SyncInfo
    public let uploads: [String: UploadInfo]

    public init(id: String, text: String? = nil, sequenceNumber: Int, sync: SyncInfo = SyncInfo(), uploads: [String: UploadInfo] = [:]) {
        self.id = id
        self.text = text
        self.sequenceNumber = sequenceNumber
        self.sync = sync
        self.uploads = uploads
    }

    public static let wireLayout = "3d888c449b3a4c344c639a31a082e38e5a5856b16d4b932bfd615e243785441d"

    public init(wire row: WireRow) throws {
        let r = CellReader(row.cells)
        self.id = row.id
        self.text = try r.opt(0, \.asText)
        self.sequenceNumber = try r.req(1, \.asInt)
        self.sync = SyncInfo(wire: row.sync)
        self.uploads = Dictionary(uniqueKeysWithValues: row.uploads.map { ($0.action, UploadInfo(wire: $0.upload)) })
    }

    public static let text = Column<MediaEntry, String?>("text")
    public static let sequenceNumber = Column<MediaEntry, Int>("sequenceNumber")
    public static func `where`(_ condition: Condition) -> Query<Self> { Query<Self>().where(condition) }
    public static func media(_ chatId: String) throws -> Query<Self> {
        let built = Query<Self>(relation: "media", params: ["chatId": try JSONValue.encode(chatId)])
        return built
    }
    @concurrent public static func create(text: String? = nil, sequenceNumber: Int, in scope: AccountScope? = nil) async throws -> Self {
        var values: [String: JSONValue] = [:]
        if let value = text { values["text"] = try JSONValue.encode(value) }
        values["sequenceNumber"] = try JSONValue.encode(sequenceNumber)
        return try await create(values: values, in: scope)
    }
    @concurrent public func update(text: String?? = nil, sequenceNumber: Int? = nil, in scope: AccountScope? = nil) async throws -> Self {
        var values: [String: JSONValue] = [:]
        if let value = text { values["text"] = try JSONValue.encode(value) }
        if let value = sequenceNumber { values["sequenceNumber"] = try JSONValue.encode(value) }
        return try await update(values: values, in: scope)
    }
}

public struct Roster: DBModel {
    public static let modelName = "Roster"
    public let id: String
    public let title: String
    public let sync: SyncInfo
    public let uploads: [String: UploadInfo]

    public init(id: String, title: String, sync: SyncInfo = SyncInfo(), uploads: [String: UploadInfo] = [:]) {
        self.id = id
        self.title = title
        self.sync = sync
        self.uploads = uploads
    }

    public static let wireLayout = "e9226eea266088346d0e13a891650ac9945faf4f4ff01bfa2daed4a6ed5e8092"

    public init(wire row: WireRow) throws {
        let r = CellReader(row.cells)
        self.id = row.id
        self.title = try r.req(0, \.asText)
        self.sync = SyncInfo(wire: row.sync)
        self.uploads = Dictionary(uniqueKeysWithValues: row.uploads.map { ($0.action, UploadInfo(wire: $0.upload)) })
    }

    public static let title = Column<Roster, String>("title")
    public static func `where`(_ condition: Condition) -> Query<Self> { Query<Self>().where(condition) }
    public static func containerMembers(_ containerId: String, order: ContainerMemberOrder = .joined) throws -> Query<Self> {
        let built = Query<Self>(relation: "container_members", params: ["containerId": try JSONValue.encode(containerId), "order": try JSONValue.encode(order)])
        return built
    }
    public static func completeMembers(_ containerId: String, order: ContainerMemberOrder = .joined) throws -> Query<Self> {
        let built = Query<Self>(relation: "complete_members", params: ["containerId": try JSONValue.encode(containerId), "order": try JSONValue.encode(order)])
        return built
    }
    @concurrent public static func create(title: String, in scope: AccountScope? = nil) async throws -> Self {
        var values: [String: JSONValue] = [:]
        values["title"] = try JSONValue.encode(title)
        return try await create(values: values, in: scope)
    }
    @concurrent public func update(title: String? = nil, in scope: AccountScope? = nil) async throws -> Self {
        var values: [String: JSONValue] = [:]
        if let value = title { values["title"] = try JSONValue.encode(value) }
        return try await update(values: values, in: scope)
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

    public init(from decoder: Decoder) throws {
        let container = try decoder.container(keyedBy: CodingKeys.self)
        self.`default` = try container.decodeIfPresent(String.self, forKey: .`default`)
        self.when = try container.decodeIfPresent(String.self, forKey: .when)
        self.container = try container.decodeIfPresent(String.self, forKey: .container)
        self.r = try container.decodeIfPresent(String.self, forKey: .r)
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
        self.url = try container.decodeIfPresent(String.self, forKey: .url)
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
        self.id = try container.decodeIfPresent(String.self, forKey: .id)
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

    public init(from decoder: Decoder) throws {
        let container = try decoder.container(keyedBy: CodingKeys.self)
        self.rewards = try container.decodeIfPresent([PricingLoadResultRewards].self, forKey: .rewards)
        self.videoIntro = try container.decodeIfPresent(PricingLoadResultVideoIntro.self, forKey: .videoIntro)
        self.tags = try container.decodeIfPresent([String].self, forKey: .tags)
        self.spec = try container.decodeIfPresent(JSONValue.self, forKey: .spec)
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

    public init(from decoder: Decoder) throws {
        let container = try decoder.container(keyedBy: CodingKeys.self)
        self.id = try container.decodeIfPresent(IDText.self, forKey: .id)?.value
        self.name = try container.decodeIfPresent(String.self, forKey: .name)
        self.price = try container.decodeIfPresent(Int.self, forKey: .price)
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
        self.enabled = try container.decodeIfPresent(Bool.self, forKey: .enabled)
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
        self.moment = try container.decodeIfPresent(MomentCreateResultMoment.self, forKey: .moment)
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
        self.id = try container.decodeIfPresent(IDText.self, forKey: .id)?.value
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
        self.id = try container.decodeIfPresent(IDText.self, forKey: .id)?.value
        self.url = try container.decodeIfPresent(String.self, forKey: .url)
        self.isSticker = try container.decodeIfPresent(Bool.self, forKey: .isSticker)
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

    public init(from decoder: Decoder) throws {
        let container = try decoder.container(keyedBy: CodingKeys.self)
        self.id = try container.decodeIfPresent(String.self, forKey: .id)
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

    public init(from decoder: Decoder) throws {
        let container = try decoder.container(keyedBy: CodingKeys.self)
        self.visualCount = try container.decodeIfPresent(Int.self, forKey: .visualCount)
        self.audioCount = try container.decodeIfPresent(Int.self, forKey: .audioCount)
        self.rewardCount = try container.decodeIfPresent(Int.self, forKey: .rewardCount)
        self.latestSequence = try container.decodeIfPresent(Int.self, forKey: .latestSequence)
    }
}

public enum Operations {
    @concurrent public static func when(`class`: String) async throws -> WhenResult {
        var variables: [String: JSONValue] = [:]
        variables["class"] = try JSONValue.encode(`class`)
        return try await DB.run("KeywordCall", variables: variables, as: WhenResult.self)
    }
    @concurrent public static func qrCodeCreate(kind: String, size: Int? = nil) async throws -> QrCodeCreateResult {
        var variables: [String: JSONValue] = [:]
        variables["kind"] = try JSONValue.encode(kind)
        if let value = size { variables["size"] = try JSONValue.encode(value) }
        return try await DB.run("QrCodeCreate", variables: ["input": JSONValue.object(variables)], as: QrCodeCreateResult.self)
    }
    @concurrent public static func signOut() async throws {
        let variables: [String: JSONValue] = [:]
        try await DB.run("SignOut", variables: variables)
    }
    @concurrent public static func stickerSend(input: StickerInput) async throws -> StickerSendResult {
        var variables: [String: JSONValue] = [:]
        variables["input"] = try JSONValue.encode(input)
        return try await DB.run("StickerSend", variables: variables, as: StickerSendResult.self)
    }
    @concurrent public static func pricingLoad() async throws -> PricingLoadResult {
        let variables: [String: JSONValue] = [:]
        return try await DB.run("Pricing", variables: variables, as: PricingLoadResult.self)
    }
    @concurrent public static func momentCreate(input: MomentInput) async throws -> MomentCreateResult {
        var variables: [String: JSONValue] = [:]
        variables["input"] = try JSONValue.encode(input)
        return try await DB.run("MomentCreate", variables: variables, as: MomentCreateResult.self)
    }
    @concurrent public static func giphyLoad(type: String, limit: Int? = nil) async throws -> [GiphyLoadResult] {
        var variables: [String: JSONValue] = [:]
        variables["type"] = try JSONValue.encode(type)
        if let value = limit { variables["limit"] = try JSONValue.encode(value) }
        return try await DB.run("Giphy", variables: variables, as: [GiphyLoadResult].self)
    }
    @concurrent public static func widgetsSend(widgets: [StickerWidgetInput]? = nil) async throws -> WidgetsSendResult {
        var variables: [String: JSONValue] = [:]
        if let value = widgets { variables["widgets"] = try JSONValue.encode(value) }
        return try await DB.run("WidgetsSend", variables: variables, as: WidgetsSendResult.self)
    }
    @concurrent public static func containerMediaLoad(containerId: String, upToSequence: Int? = nil) async throws -> ContainerMediaLoadResult {
        var variables: [String: JSONValue] = [:]
        variables["containerId"] = try JSONValue.encode(containerId)
        if let value = upToSequence { variables["upToSequence"] = try JSONValue.encode(value) }
        return try await DB.run("containerMedia", variables: variables, as: ContainerMediaLoadResult.self)
    }
}


public extension BlobUpload {
    static let videoIntro = BlobUpload(name: "video_intro", create: "BlobCreate")
}
