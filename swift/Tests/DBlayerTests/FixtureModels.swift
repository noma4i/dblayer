import Foundation
import DBlayer

public struct Chat: DBModel {
    public static let modelName = "Chat"
    public let id: String
    public let title: String
    public let lastMessageAt: String?
    public let lastMessageId: String?
    public let sync: SyncInfo
    public let upload: UploadInfo?

    public init(id: String, title: String, lastMessageAt: String? = nil, lastMessageId: String? = nil, sync: SyncInfo = SyncInfo(), upload: UploadInfo? = nil) {
        self.id = id
        self.title = title
        self.lastMessageAt = lastMessageAt
        self.lastMessageId = lastMessageId
        self.sync = sync
        self.upload = upload
    }

    public static let title = Column<Chat, String>("title")
    public static let lastMessageAt = Column<Chat, String?>("lastMessageAt")
    public static let lastMessageId = Column<Chat, String?>("lastMessageId")
    public static func find(_ id: String) -> LiveSource<Self?> { Query<Self>().find(id) }
    public static func `where`(_ condition: Condition) -> Query<Self> { Query<Self>().where(condition) }
    public static var recent: Query<Self> {
        var built = Query<Self>(relation: "recent", params: [:])
        built = built.order(Order(field: "lastMessageAt", direction: "desc"))
        return built
    }
    public static func create(title: String, lastMessageAt: String? = nil, lastMessageId: String? = nil) async throws -> Self {
        var values: [String: JSONValue] = [:]
        values["title"] = JSONValue.encode(title)
        if let value = lastMessageAt { values["lastMessageAt"] = JSONValue.encode(value) }
        if let value = lastMessageId { values["lastMessageId"] = JSONValue.encode(value) }
        return try await create(values: values)
    }
    public func update(title: String? = nil, lastMessageAt: String?? = nil, lastMessageId: String?? = nil) async throws -> Self {
        var values: [String: JSONValue] = [:]
        if let value = title { values["title"] = JSONValue.encode(value) }
        if let value = lastMessageAt { values["lastMessageAt"] = JSONValue.encode(value) }
        if let value = lastMessageId { values["lastMessageId"] = JSONValue.encode(value) }
        return try await update(values: values)
    }
    public var messages: LiveSource<[Message]> { related("messages", as: Message.self) }
    public var lastMessage: LiveSource<[Message]> { related("lastMessage", as: Message.self) }
}

public struct Message: DBModel {
    public static let modelName = "Message"
    public let id: String
    public let chatId: String
    public let authorId: String
    public let text: String
    public let createdAt: String
    public let sync: SyncInfo
    public let upload: UploadInfo?

    public init(id: String, chatId: String, authorId: String, text: String, createdAt: String, sync: SyncInfo = SyncInfo(), upload: UploadInfo? = nil) {
        self.id = id
        self.chatId = chatId
        self.authorId = authorId
        self.text = text
        self.createdAt = createdAt
        self.sync = sync
        self.upload = upload
    }

    public static let chatId = Column<Message, String>("chatId")
    public static let authorId = Column<Message, String>("authorId")
    public static let text = Column<Message, String>("text")
    public static let createdAt = Column<Message, String>("createdAt")
    public static func find(_ id: String) -> LiveSource<Self?> { Query<Self>().find(id) }
    public static func `where`(_ condition: Condition) -> Query<Self> { Query<Self>().where(condition) }
    public static func byChat(_ chatId: String) -> Query<Self> {
        var built = Query<Self>(relation: "by_chat", params: ["chatId": JSONValue.encode(chatId)])
        built = built.where(Condition.equal("chatId", JSONValue.encode(chatId)))
        built = built.order(Order(field: "createdAt", direction: "asc"))
        return built
    }
    public static var history: Query<Self> {
        let built = Query<Self>(relation: "history", params: [:])
        return built
    }
    public static func create(chatId: String, authorId: String, text: String) async throws -> Self {
        var values: [String: JSONValue] = [:]
        values["chatId"] = JSONValue.encode(chatId)
        values["authorId"] = JSONValue.encode(authorId)
        values["text"] = JSONValue.encode(text)
        return try await create(values: values)
    }
    public func update(chatId: String? = nil, authorId: String? = nil, text: String? = nil, createdAt: String? = nil) async throws -> Self {
        var values: [String: JSONValue] = [:]
        if let value = chatId { values["chatId"] = JSONValue.encode(value) }
        if let value = authorId { values["authorId"] = JSONValue.encode(value) }
        if let value = text { values["text"] = JSONValue.encode(value) }
        if let value = createdAt { values["createdAt"] = JSONValue.encode(value) }
        return try await update(values: values)
    }
    public var chat: LiveSource<[Chat]> { related("chat", as: Chat.self) }
}

public struct Moment: DBModel {
    public static let modelName = "Moment"
    public let id: String
    public let uuid: String
    public let userId: String?
    public let text: String?
    public let likedByMe: Bool
    public let media: JSONValue
    public let tone: String?
    public let createdAt: String
    public let sync: SyncInfo
    public let upload: UploadInfo?

    public init(id: String, uuid: String, userId: String? = nil, text: String? = nil, likedByMe: Bool = false, media: JSONValue, tone: String? = nil, createdAt: String, sync: SyncInfo = SyncInfo(), upload: UploadInfo? = nil) {
        self.id = id
        self.uuid = uuid
        self.userId = userId
        self.text = text
        self.likedByMe = likedByMe
        self.media = media
        self.tone = tone
        self.createdAt = createdAt
        self.sync = sync
        self.upload = upload
    }

    public static let uuid = Column<Moment, String>("uuid")
    public static let userId = Column<Moment, String?>("userId")
    public static let text = Column<Moment, String?>("text")
    public static let likedByMe = Column<Moment, Bool>("likedByMe")
    public static let media = Column<Moment, JSONValue>("media")
    public static let tone = Column<Moment, String?>("tone")
    public static let createdAt = Column<Moment, String>("createdAt")
    public static func find(_ id: String) -> LiveSource<Self?> { Query<Self>().find(id) }
    public static func `where`(_ condition: Condition) -> Query<Self> { Query<Self>().where(condition) }
    public static var recent: Query<Self> {
        var built = Query<Self>(relation: "recent", params: [:])
        built = built.order(Order(field: "createdAt", direction: "desc"))
        return built
    }
    public static func byUser(_ userId: String?) -> Query<Self> {
        var built = Query<Self>(relation: "by_user", params: ["userId": JSONValue.encode(userId)])
        built = built.where(Condition.equal("userId", JSONValue.encode(userId)))
        built = built.order(Order(field: "createdAt", direction: "desc"))
        return built
    }
    public static var feed: Query<Self> {
        let built = Query<Self>(relation: "feed", params: [:])
        return built
    }
    public static func create(text: String? = nil, media: JSONValue? = nil) async throws -> Self {
        var values: [String: JSONValue] = [:]
        if let value = text { values["text"] = JSONValue.encode(value) }
        if let value = media { values["media"] = JSONValue.encode(value) }
        return try await create(values: values)
    }
    public func update(uuid: String? = nil, userId: String?? = nil, text: String?? = nil, likedByMe: Bool? = nil, media: JSONValue? = nil, createdAt: String? = nil) async throws -> Self {
        var values: [String: JSONValue] = [:]
        if let value = uuid { values["uuid"] = JSONValue.encode(value) }
        if let value = userId { values["userId"] = JSONValue.encode(value) }
        if let value = text { values["text"] = JSONValue.encode(value) }
        if let value = likedByMe { values["likedByMe"] = JSONValue.encode(value) }
        if let value = media { values["media"] = JSONValue.encode(value) }
        if let value = createdAt { values["createdAt"] = JSONValue.encode(value) }
        return try await update(values: values)
    }
    public var author: LiveSource<[User]> { related("author", as: User.self) }
    public var vibes: LiveSource<[Vibe]> { related("vibes", as: Vibe.self) }
    public func like() async throws -> Self {
        let values = JSONValue.parse(#"{"likedByMe":true}"#).objectValue
        return try await act("like", values: values, args: [:])
    }
    public func share() async throws -> ActionAnswer<Self, MomentShareResult> {
        let values = JSONValue.parse(#"{}"#).objectValue
        return try await actAnswering("share", values: values, args: [:])
    }
}

public struct MomentShareResult: Codable, Sendable {
    public let copies: Int?
    public let successMessage: String?
}

public struct NestedReference: DBModel {
    public static let modelName = "NestedReference"
    public let id: String
    public let values: [NestedValue]
    public let sync: SyncInfo
    public let upload: UploadInfo?

    public init(id: String, values: [NestedValue], sync: SyncInfo = SyncInfo(), upload: UploadInfo? = nil) {
        self.id = id
        self.values = values
        self.sync = sync
        self.upload = upload
    }

    public static let values = Column<NestedReference, [NestedValue]>("values")
    public static func find(_ id: String) -> LiveSource<Self?> { Query<Self>().find(id) }
    public static func `where`(_ condition: Condition) -> Query<Self> { Query<Self>().where(condition) }
    public static func create(values: [NestedValue]? = nil) async throws -> Self {
        var values_: [String: JSONValue] = [:]
        if let value = values { values_["values"] = JSONValue.encode(value) }
        return try await create(values: values_)
    }
    public func update(values: [NestedValue]? = nil) async throws -> Self {
        var values_: [String: JSONValue] = [:]
        if let value = values { values_["values"] = JSONValue.encode(value) }
        return try await update(values: values_)
    }
}

public struct NestedSample: DBModel {
    public static let modelName = "NestedSample"
    public let id: String
    public let value: NestedValue?
    public let values: [NestedValue]
    public let tree: NestedTree?
    public let replyTo: MessageReplyNode?
    public let sync: SyncInfo
    public let upload: UploadInfo?

    public init(id: String, value: NestedValue? = nil, values: [NestedValue], tree: NestedTree? = nil, replyTo: MessageReplyNode? = nil, sync: SyncInfo = SyncInfo(), upload: UploadInfo? = nil) {
        self.id = id
        self.value = value
        self.values = values
        self.tree = tree
        self.replyTo = replyTo
        self.sync = sync
        self.upload = upload
    }

    public static let value = Column<NestedSample, NestedValue?>("value")
    public static let values = Column<NestedSample, [NestedValue]>("values")
    public static let tree = Column<NestedSample, NestedTree?>("tree")
    public static let replyTo = Column<NestedSample, MessageReplyNode?>("replyTo")
    public static func find(_ id: String) -> LiveSource<Self?> { Query<Self>().find(id) }
    public static func `where`(_ condition: Condition) -> Query<Self> { Query<Self>().where(condition) }
    public static func create(value: NestedValue? = nil, values: [NestedValue]? = nil, tree: NestedTree? = nil, replyTo: MessageReplyNode? = nil) async throws -> Self {
        var values_: [String: JSONValue] = [:]
        if let value = value { values_["value"] = JSONValue.encode(value) }
        if let value = values { values_["values"] = JSONValue.encode(value) }
        if let value = tree { values_["tree"] = JSONValue.encode(value) }
        if let value = replyTo { values_["replyTo"] = JSONValue.encode(value) }
        return try await create(values: values_)
    }
    public func update(value: NestedValue?? = nil, values: [NestedValue]? = nil, tree: NestedTree?? = nil, replyTo: MessageReplyNode?? = nil) async throws -> Self {
        var values_: [String: JSONValue] = [:]
        if let value = value { values_["value"] = JSONValue.encode(value) }
        if let value = values { values_["values"] = JSONValue.encode(value) }
        if let value = tree { values_["tree"] = JSONValue.encode(value) }
        if let value = replyTo { values_["replyTo"] = JSONValue.encode(value) }
        return try await update(values: values_)
    }
}

public struct Presence: DBModel {
    public static let modelName = "Presence"
    public let id: String
    public let userId: String
    public let room: String
    public let sync: SyncInfo
    public let upload: UploadInfo?

    public init(id: String, userId: String, room: String, sync: SyncInfo = SyncInfo(), upload: UploadInfo? = nil) {
        self.id = id
        self.userId = userId
        self.room = room
        self.sync = sync
        self.upload = upload
    }

    public static let userId = Column<Presence, String>("userId")
    public static let room = Column<Presence, String>("room")
    public static func find(_ id: String) -> LiveSource<Self?> { Query<Self>().find(id) }
    public static func `where`(_ condition: Condition) -> Query<Self> { Query<Self>().where(condition) }
    public static func forUser(_ userId: String) -> Query<Self> {
        var built = Query<Self>(relation: "for_user", params: ["userId": JSONValue.encode(userId)])
        built = built.where(Condition.equal("userId", JSONValue.encode(userId)))
        return built
    }
    public static var online: Query<Self> {
        let built = Query<Self>(relation: "online", params: [:])
        return built
    }
    public static func create(userId: String, room: String) async throws -> Self {
        var values: [String: JSONValue] = [:]
        values["userId"] = JSONValue.encode(userId)
        values["room"] = JSONValue.encode(room)
        return try await create(values: values)
    }
    public func update(userId: String? = nil, room: String? = nil) async throws -> Self {
        var values: [String: JSONValue] = [:]
        if let value = userId { values["userId"] = JSONValue.encode(value) }
        if let value = room { values["room"] = JSONValue.encode(value) }
        return try await update(values: values)
    }
    public var user: LiveSource<[User]> { related("user", as: User.self) }
}

public struct User: DBSingleton {
    public static let modelName = "User"
    public let id: String
    public let name: String
    public let avatar: JSONValue?
    public let birthday: String?
    public let typing: Bool
    public let mood: String?
    public let sync: SyncInfo
    public let upload: UploadInfo?

    public init(id: String, name: String, avatar: JSONValue? = nil, birthday: String? = nil, typing: Bool = false, mood: String? = nil, sync: SyncInfo = SyncInfo(), upload: UploadInfo? = nil) {
        self.id = id
        self.name = name
        self.avatar = avatar
        self.birthday = birthday
        self.typing = typing
        self.mood = mood
        self.sync = sync
        self.upload = upload
    }

    public static let name = Column<User, String>("name")
    public static let avatar = Column<User, JSONValue?>("avatar")
    public static let birthday = Column<User, String?>("birthday")
    public static func find(_ id: String) -> LiveSource<Self?> { Query<Self>().find(id) }
    public static func `where`(_ condition: Condition) -> Query<Self> { Query<Self>().where(condition) }
    public static var profile: Query<Self> {
        let built = Query<Self>(relation: "profile", params: [:])
        return built
    }
    public static var singleton: Query<Self> { profile }
    public static func create(name: String, avatar: JSONValue? = nil, birthday: String? = nil) async throws -> Self {
        var values: [String: JSONValue] = [:]
        values["name"] = JSONValue.encode(name)
        if let value = avatar { values["avatar"] = JSONValue.encode(value) }
        if let value = birthday { values["birthday"] = JSONValue.encode(value) }
        return try await create(values: values)
    }
    public func update(name: String? = nil, avatar: JSONValue?? = nil, birthday: String?? = nil) async throws -> Self {
        var values: [String: JSONValue] = [:]
        if let value = name { values["name"] = JSONValue.encode(value) }
        if let value = avatar { values["avatar"] = JSONValue.encode(value) }
        if let value = birthday { values["birthday"] = JSONValue.encode(value) }
        return try await update(values: values)
    }
    public func set(typing: Bool? = nil, mood: String?? = nil) async throws {
        var values: [String: JSONValue] = [:]
        if let value = typing { values["typing"] = JSONValue.encode(value) }
        if let value = mood { values["mood"] = JSONValue.encode(value) }
        try await set(values: values)
    }
}

public struct Vibe: DBModel {
    public static let modelName = "Vibe"
    public let id: String
    public let momentId: String
    public let userId: String
    public let kind: String
    public let sync: SyncInfo
    public let upload: UploadInfo?

    public init(id: String, momentId: String, userId: String, kind: String, sync: SyncInfo = SyncInfo(), upload: UploadInfo? = nil) {
        self.id = id
        self.momentId = momentId
        self.userId = userId
        self.kind = kind
        self.sync = sync
        self.upload = upload
    }

    public static let momentId = Column<Vibe, String>("momentId")
    public static let userId = Column<Vibe, String>("userId")
    public static let kind = Column<Vibe, String>("kind")
    public static func find(_ id: String) -> LiveSource<Self?> { Query<Self>().find(id) }
    public static func `where`(_ condition: Condition) -> Query<Self> { Query<Self>().where(condition) }
    public static func create(momentId: String, userId: String, kind: String) async throws -> Self {
        var values: [String: JSONValue] = [:]
        values["momentId"] = JSONValue.encode(momentId)
        values["userId"] = JSONValue.encode(userId)
        values["kind"] = JSONValue.encode(kind)
        return try await create(values: values)
    }
    public func update(momentId: String? = nil, userId: String? = nil, kind: String? = nil) async throws -> Self {
        var values: [String: JSONValue] = [:]
        if let value = momentId { values["momentId"] = JSONValue.encode(value) }
        if let value = userId { values["userId"] = JSONValue.encode(value) }
        if let value = kind { values["kind"] = JSONValue.encode(value) }
        return try await update(values: values)
    }
}

public struct NestedValue: Codable, Sendable, Equatable {
    public let title: String
    public let note: String?
    public let nullableDefault: String?
    public let ownerId: String?
    public let count: Int
    public let ratio: Double
    public let enabled: Bool
    public let createdAt: String?
    public let state: String
    public let labels: [String]
    public let counts: [Int]
    public let ratios: [Double]
    public let flags: [Bool]
    public let dates: [String]
    public let owners: [String]
    public let payload: JSONValue?
    public let rawValue: JSONValue
    public let preferences: NestedValuePreferences
    public let presets: [NestedValuePreferences]
    public let metadata: JSONValue?
    public let values: JSONValue?
    public let child: NestedValueChild?
    public let children: [NestedValueChild]

    public init(title: String, note: String? = nil, nullableDefault: String? = "default", ownerId: String? = nil, count: Int = 2, ratio: Double = 1.5, enabled: Bool = false, createdAt: String? = nil, state: String = "ready", labels: [String] = [], counts: [Int] = [], ratios: [Double] = [], flags: [Bool] = [], dates: [String] = [], owners: [String] = [], payload: JSONValue? = nil, rawValue: JSONValue = .object(["free": .array([.number(1), .bool(true), .null])]), preferences: NestedValuePreferences = NestedValuePreferences(label: "base"), presets: [NestedValuePreferences] = [NestedValuePreferences(label: "first")], metadata: JSONValue? = nil, values: JSONValue? = nil, child: NestedValueChild? = nil, children: [NestedValueChild] = []) {
        self.title = title
        self.note = note
        self.nullableDefault = nullableDefault
        self.ownerId = ownerId
        self.count = count
        self.ratio = ratio
        self.enabled = enabled
        self.createdAt = createdAt
        self.state = state
        self.labels = labels
        self.counts = counts
        self.ratios = ratios
        self.flags = flags
        self.dates = dates
        self.owners = owners
        self.payload = payload
        self.rawValue = rawValue
        self.preferences = preferences
        self.presets = presets
        self.metadata = metadata
        self.values = values
        self.child = child
        self.children = children
    }

    private enum CodingKeys: String, CodingKey {
        case title = "title"
        case note = "note"
        case nullableDefault = "nullableDefault"
        case ownerId = "owner_id"
        case count = "count"
        case ratio = "ratio"
        case enabled = "enabled"
        case createdAt = "createdAt"
        case state = "state"
        case labels = "labels"
        case counts = "counts"
        case ratios = "ratios"
        case flags = "flags"
        case dates = "dates"
        case owners = "owners"
        case payload = "payload"
        case rawValue = "rawValue"
        case preferences = "preferences"
        case presets = "presets"
        case metadata = "metadata"
        case values = "values"
        case child = "child"
        case children = "children"
    }

    public init(from decoder: Decoder) throws {
        let container = try decoder.container(keyedBy: CodingKeys.self)
        title = try container.decode(String.self, forKey: .title)
        note = try container.decodeIfPresent(String.self, forKey: .note)
        nullableDefault = try (container.contains(.nullableDefault) ? container.decodeIfPresent(String.self, forKey: .nullableDefault) : "default")
        ownerId = try container.decodeIfPresent(IDText.self, forKey: .ownerId)?.value
        count = try container.decodeIfPresent(Int.self, forKey: .count) ?? 2
        ratio = try container.decodeIfPresent(Double.self, forKey: .ratio) ?? 1.5
        enabled = try container.decodeIfPresent(Bool.self, forKey: .enabled) ?? false
        createdAt = try container.decodeIfPresent(String.self, forKey: .createdAt)
        state = try container.decodeIfPresent(String.self, forKey: .state) ?? "ready"
        labels = try container.decodeIfPresent([String].self, forKey: .labels) ?? []
        counts = try container.decodeIfPresent([Int].self, forKey: .counts) ?? []
        ratios = try container.decodeIfPresent([Double].self, forKey: .ratios) ?? []
        flags = try container.decodeIfPresent([Bool].self, forKey: .flags) ?? []
        dates = try container.decodeIfPresent([String].self, forKey: .dates) ?? []
        owners = try container.decodeIfPresent([IDText].self, forKey: .owners)?.map(\.value) ?? []
        payload = try container.decodeIfPresent(JSONValue.self, forKey: .payload)
        rawValue = try container.decodeIfPresent(JSONValue.self, forKey: .rawValue) ?? .object(["free": .array([.number(1), .bool(true), .null])])
        preferences = try container.decodeIfPresent(NestedValuePreferences.self, forKey: .preferences) ?? NestedValuePreferences(label: "base")
        presets = try container.decodeIfPresent([NestedValuePreferences].self, forKey: .presets) ?? [NestedValuePreferences(label: "first")]
        metadata = try container.decodeIfPresent(JSONValue.self, forKey: .metadata)
        values = try container.decodeIfPresent(JSONValue.self, forKey: .values)
        child = try container.decodeIfPresent(NestedValueChild.self, forKey: .child)
        children = try container.decodeIfPresent([NestedValueChild].self, forKey: .children) ?? []
    }
}

public struct NestedValuePreferences: Codable, Sendable, Equatable {
    public let label: String
    public let active: Bool

    public init(label: String, active: Bool = true) {
        self.label = label
        self.active = active
    }

    private enum CodingKeys: String, CodingKey {
        case label = "label"
        case active = "active"
    }

    public init(from decoder: Decoder) throws {
        let container = try decoder.container(keyedBy: CodingKeys.self)
        label = try container.decode(String.self, forKey: .label)
        active = try container.decodeIfPresent(Bool.self, forKey: .active) ?? true
    }
}

public struct NestedValueChild: Codable, Sendable, Equatable {
    public let body: String?
    public let horizontal: Double

    public init(body: String? = nil, horizontal: Double = 0.0) {
        self.body = body
        self.horizontal = horizontal
    }

    private enum CodingKeys: String, CodingKey {
        case body = "body"
        case horizontal = "x"
    }

    public init(from decoder: Decoder) throws {
        let container = try decoder.container(keyedBy: CodingKeys.self)
        body = try container.decodeIfPresent(String.self, forKey: .body)
        horizontal = try container.decodeIfPresent(Double.self, forKey: .horizontal) ?? 0.0
    }
}

public struct NestedTree: Codable, Sendable, Equatable {
    public let name: String
    public let children: [NestedTree]

    public init(name: String, children: [NestedTree] = []) {
        self.name = name
        self.children = children
    }

    private enum CodingKeys: String, CodingKey {
        case name = "name"
        case children = "children"
    }

    public init(from decoder: Decoder) throws {
        let container = try decoder.container(keyedBy: CodingKeys.self)
        name = try container.decode(String.self, forKey: .name)
        children = try container.decodeIfPresent([NestedTree].self, forKey: .children) ?? []
    }
}

public struct MessageReplyNode: Codable, Sendable, Equatable {
    public let id: String?
    public let body: String?
    public let kind: String?
    public let userId: String?
    public let user: MessageUserRef?
    public let media: NestedMedia?

    public init(id: String? = nil, body: String? = nil, kind: String? = nil, userId: String? = nil, user: MessageUserRef? = nil, media: NestedMedia? = nil) {
        self.id = id
        self.body = body
        self.kind = kind
        self.userId = userId
        self.user = user
        self.media = media
    }

    private enum CodingKeys: String, CodingKey {
        case id = "id"
        case body = "body"
        case kind = "kind"
        case userId = "userId"
        case user = "user"
        case media = "media"
    }

    public init(from decoder: Decoder) throws {
        let container = try decoder.container(keyedBy: CodingKeys.self)
        id = try container.decodeIfPresent(IDText.self, forKey: .id)?.value
        body = try container.decodeIfPresent(String.self, forKey: .body)
        kind = try container.decodeIfPresent(String.self, forKey: .kind)
        userId = try container.decodeIfPresent(IDText.self, forKey: .userId)?.value
        user = try container.decodeIfPresent(MessageUserRef.self, forKey: .user)
        media = try container.decodeIfPresent(NestedMedia.self, forKey: .media)
    }
}

public struct MessageUserRef: Codable, Sendable, Equatable {
    public let id: String?
    public let fullName: String?
    public let name: String?
    public let username: String?
    public let avatarUrl: String?

    public init(id: String? = nil, fullName: String? = nil, name: String? = nil, username: String? = nil, avatarUrl: String? = nil) {
        self.id = id
        self.fullName = fullName
        self.name = name
        self.username = username
        self.avatarUrl = avatarUrl
    }

    private enum CodingKeys: String, CodingKey {
        case id = "id"
        case fullName = "fullName"
        case name = "name"
        case username = "username"
        case avatarUrl = "avatarUrl"
    }

    public init(from decoder: Decoder) throws {
        let container = try decoder.container(keyedBy: CodingKeys.self)
        id = try container.decodeIfPresent(IDText.self, forKey: .id)?.value
        fullName = try container.decodeIfPresent(String.self, forKey: .fullName)
        name = try container.decodeIfPresent(String.self, forKey: .name)
        username = try container.decodeIfPresent(String.self, forKey: .username)
        avatarUrl = try container.decodeIfPresent(String.self, forKey: .avatarUrl)
    }
}

public struct NestedMedia: Codable, Sendable, Equatable {
    public let id: String
    public let fileUrl: String?

    public init(id: String = "", fileUrl: String? = nil) {
        self.id = id
        self.fileUrl = fileUrl
    }

    private enum CodingKeys: String, CodingKey {
        case id = "id"
        case fileUrl = "fileUrl"
    }

    public init(from decoder: Decoder) throws {
        let container = try decoder.container(keyedBy: CodingKeys.self)
        id = try container.decodeIfPresent(IDText.self, forKey: .id)?.value ?? ""
        fileUrl = try container.decodeIfPresent(String.self, forKey: .fileUrl)
    }
}

public let modelsJSON = #"[{"key":"Chat","fields":[{"name":"id","kind":{"type":"scalar","kind":"id"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"title","kind":{"type":"scalar","kind":"str"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"lastMessageAt","kind":{"type":"scalar","kind":"date"},"nullable":true,"optional":false,"default":null,"indexed":true,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"lastMessageId","kind":{"type":"scalar","kind":"id"},"nullable":true,"optional":false,"default":null,"indexed":true,"id_from_parent":null,"from":null,"app_type":null,"merge":null}],"associations":[{"name":"messages","kind":"has_many","target":"Message","foreign_key":"chatId","dependent":"destroy","touches_parent":false,"sets":{},"newer":[],"from":null},{"name":"lastMessage","kind":"belongs_to","target":"Message","foreign_key":"lastMessageId","dependent":null,"touches_parent":false,"sets":{},"newer":[],"from":null}],"relations":[{"name":"recent","by_columns":[],"text_params":[],"remote":null,"is_global":true,"sort":{"fields":[{"column":"lastMessageAt","direction":"desc"}]},"joins_events":false,"joins_created":false}],"composite_indexes":[],"actions":[{"name":"create","root_shape":"upsert","on_failure":"rollback","has_optimistic":true,"has_temp_row_retention":true,"has_cascade_dependents":false,"has_once":false,"dedupe_key":null,"members":[],"mutation":"CreateChat","variables":{},"optimistic":{},"refetch":[],"args":{},"local":[],"correlate":[],"ignore_response":false,"wrap":null,"root":null,"lands":[],"result":{}}],"ephemeral":[],"storage":"disk","expire_after":null,"listens":[]},{"key":"Message","fields":[{"name":"id","kind":{"type":"scalar","kind":"id"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"chatId","kind":{"type":"scalar","kind":"id"},"nullable":false,"optional":false,"default":null,"indexed":true,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"authorId","kind":{"type":"scalar","kind":"id"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"text","kind":{"type":"scalar","kind":"str"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"createdAt","kind":{"type":"scalar","kind":"date"},"nullable":false,"optional":false,"default":null,"indexed":true,"id_from_parent":null,"from":null,"app_type":null,"merge":null}],"associations":[{"name":"chat","kind":"belongs_to","target":"Chat","foreign_key":"chatId","dependent":null,"touches_parent":false,"sets":{"lastMessageAt":"createdAt","lastMessageId":"id"},"newer":[{"child":"createdAt","parent":"lastMessageAt"}],"from":null}],"relations":[{"name":"by_chat","by_columns":["chatId"],"text_params":[],"remote":null,"is_global":false,"sort":{"fields":[{"column":"createdAt","direction":"asc"}]},"joins_events":false,"joins_created":false},{"name":"history","by_columns":[],"text_params":[],"remote":{"operation":"ChatMessages","shape":{"type":"connection","direction":"backward","coverage":"page","max_pages":null,"cursor":null},"stale_time":"1.minute","empty_stale_time":"10.seconds","selects_page_info":true,"updates":[],"page_size":30,"variables":{},"root":null},"is_global":true,"sort":"server_order","joins_events":false,"joins_created":false}],"composite_indexes":[],"actions":[{"name":"create","root_shape":"upsert","on_failure":"rollback","has_optimistic":true,"has_temp_row_retention":true,"has_cascade_dependents":false,"has_once":false,"dedupe_key":null,"members":[],"mutation":"CreateMessage","variables":{"chatId":"chatId","text":"text"},"optimistic":{},"refetch":[],"args":{},"local":["authorId"],"correlate":[],"ignore_response":false,"wrap":null,"root":null,"lands":[],"result":{}}],"ephemeral":[],"storage":"disk","expire_after":null,"listens":[]},{"key":"Moment","fields":[{"name":"id","kind":{"type":"scalar","kind":"id"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"uuid","kind":{"type":"scalar","kind":"str"},"nullable":false,"optional":false,"default":null,"indexed":true,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"userId","kind":{"type":"scalar","kind":"id"},"nullable":true,"optional":false,"default":null,"indexed":true,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"text","kind":{"type":"scalar","kind":"str"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"likedByMe","kind":{"type":"scalar","kind":"bool"},"nullable":false,"optional":false,"default":false,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"media","kind":{"type":"object"},"nullable":false,"optional":false,"default":{},"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"tone","kind":{"type":"scalar","kind":"str"},"nullable":false,"optional":true,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":{"type":"local"}},{"name":"createdAt","kind":{"type":"scalar","kind":"date"},"nullable":false,"optional":false,"default":null,"indexed":true,"id_from_parent":null,"from":null,"app_type":null,"merge":null}],"associations":[{"name":"author","kind":"belongs_to","target":"User","foreign_key":"userId","dependent":null,"touches_parent":true,"sets":{},"newer":[],"from":null},{"name":"vibes","kind":"has_many","target":"Vibe","foreign_key":"momentId","dependent":"destroy","touches_parent":false,"sets":{},"newer":[],"from":null}],"relations":[{"name":"recent","by_columns":[],"text_params":[],"remote":null,"is_global":true,"sort":{"fields":[{"column":"createdAt","direction":"desc"}]},"joins_events":false,"joins_created":false},{"name":"by_user","by_columns":["userId"],"text_params":[],"remote":null,"is_global":false,"sort":{"fields":[{"column":"createdAt","direction":"desc"}]},"joins_events":false,"joins_created":false},{"name":"feed","by_columns":[],"text_params":[],"remote":{"operation":"MomentsFeed","shape":{"type":"connection","direction":"forward","coverage":"page","max_pages":null,"cursor":null},"stale_time":"5.minutes","empty_stale_time":null,"selects_page_info":true,"updates":[{"subscription":"MomentEvents","deleted_by":"destroyed","set":{},"removes":false,"debounce":null,"root":null,"id":null}],"page_size":20,"variables":{},"root":null},"is_global":true,"sort":"server_order","joins_events":false,"joins_created":false}],"composite_indexes":[["userId","createdAt"]],"actions":[{"name":"create","root_shape":"upsert","on_failure":"rollback","has_optimistic":true,"has_temp_row_retention":true,"has_cascade_dependents":false,"has_once":false,"dedupe_key":null,"members":[],"mutation":"CreateMoment","variables":{"media":"media","text":"text"},"optimistic":{},"refetch":[],"args":{},"local":[],"correlate":[],"ignore_response":false,"wrap":null,"root":null,"lands":[],"result":{}},{"name":"update","root_shape":"upsert","on_failure":"rollback","has_optimistic":false,"has_temp_row_retention":false,"has_cascade_dependents":false,"has_once":false,"dedupe_key":null,"members":[],"mutation":"UpdateMoment","variables":{},"optimistic":{},"refetch":[],"args":{},"local":[],"correlate":[],"ignore_response":false,"wrap":null,"root":null,"lands":[],"result":{}},{"name":"destroy","root_shape":"destroy","on_failure":"rollback","has_optimistic":false,"has_temp_row_retention":false,"has_cascade_dependents":false,"has_once":false,"dedupe_key":null,"members":[],"mutation":"DeleteMoment","variables":{},"optimistic":{},"refetch":[],"args":{},"local":[],"correlate":[],"ignore_response":false,"wrap":null,"root":null,"lands":[],"result":{}},{"name":"like","root_shape":"upsert","on_failure":"rollback","has_optimistic":false,"has_temp_row_retention":false,"has_cascade_dependents":false,"has_once":false,"dedupe_key":null,"members":[],"mutation":"LikeMoment","variables":{"momentId":"id"},"optimistic":{"likedByMe":true},"refetch":["feed"],"args":{},"local":[],"correlate":[],"ignore_response":false,"wrap":null,"root":null,"lands":[],"result":{}},{"name":"share","root_shape":"upsert","on_failure":"rollback","has_optimistic":false,"has_temp_row_retention":false,"has_cascade_dependents":false,"has_once":false,"dedupe_key":null,"members":[],"mutation":"ShareMoment","variables":{"momentId":"id"},"optimistic":{},"refetch":[],"args":{},"local":[],"correlate":[],"ignore_response":false,"wrap":null,"root":"shareMoment.moment","lands":[],"result":{"copies":"int","successMessage":"string"}}],"ephemeral":[],"storage":"disk","expire_after":null,"listens":[]},{"key":"NestedReference","fields":[{"name":"id","kind":{"type":"scalar","kind":"id"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"values","kind":{"type":"array"},"nullable":false,"optional":false,"default":[],"indexed":false,"id_from_parent":null,"from":null,"app_type":"NestedValue","merge":null}],"associations":[],"relations":[],"composite_indexes":[],"actions":[],"ephemeral":[],"storage":"local","expire_after":null,"listens":[]},{"key":"NestedSample","fields":[{"name":"id","kind":{"type":"scalar","kind":"id"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"value","kind":{"type":"object"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"NestedValue","merge":null,"fields":[{"name":"title","kind":{"type":"scalar","kind":"str"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"note","kind":{"type":"scalar","kind":"str"},"nullable":false,"optional":true,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"nullableDefault","kind":{"type":"scalar","kind":"str"},"nullable":false,"optional":true,"default":"default","indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"ownerId","kind":{"type":"scalar","kind":"id"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":"owner_id","app_type":null,"merge":null},{"name":"count","kind":{"type":"scalar","kind":"int"},"nullable":false,"optional":false,"default":2,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"ratio","kind":{"type":"scalar","kind":"num"},"nullable":false,"optional":false,"default":1.5,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"enabled","kind":{"type":"scalar","kind":"bool"},"nullable":false,"optional":false,"default":false,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"createdAt","kind":{"type":"scalar","kind":"date"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"state","kind":{"type":"enum","values":["ready","done"]},"nullable":false,"optional":false,"default":"ready","indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"labels","kind":{"type":"list","element":"str"},"nullable":false,"optional":false,"default":[],"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"counts","kind":{"type":"list","element":"int"},"nullable":false,"optional":false,"default":[],"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"ratios","kind":{"type":"list","element":"num"},"nullable":false,"optional":false,"default":[],"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"flags","kind":{"type":"list","element":"bool"},"nullable":false,"optional":false,"default":[],"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"dates","kind":{"type":"list","element":"date"},"nullable":false,"optional":false,"default":[],"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"owners","kind":{"type":"list","element":"id"},"nullable":false,"optional":false,"default":[],"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"payload","kind":{"type":"raw"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"rawValue","kind":{"type":"raw"},"nullable":false,"optional":false,"default":{"free":[1,true,null]},"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"preferences","kind":{"type":"object"},"nullable":false,"optional":false,"default":{"label":"base"},"indexed":false,"id_from_parent":null,"from":null,"app_type":"NestedValuePreferences","merge":null,"fields":[{"name":"label","kind":{"type":"scalar","kind":"str"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"active","kind":{"type":"scalar","kind":"bool"},"nullable":false,"optional":false,"default":true,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null}]},{"name":"presets","kind":{"type":"array"},"nullable":false,"optional":false,"default":[{"label":"first"}],"indexed":false,"id_from_parent":null,"from":null,"app_type":"NestedValuePreferences","merge":null},{"name":"metadata","kind":{"type":"object"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"values","kind":{"type":"array"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"child","kind":{"type":"object"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"NestedValueChild","merge":null,"fields":[{"name":"body","kind":{"type":"scalar","kind":"str"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"horizontal","kind":{"type":"scalar","kind":"num"},"nullable":false,"optional":false,"default":0,"indexed":false,"id_from_parent":null,"from":"x","app_type":null,"merge":null}]},{"name":"children","kind":{"type":"array"},"nullable":false,"optional":false,"default":[],"indexed":false,"id_from_parent":null,"from":null,"app_type":"NestedValueChild","merge":null}]},{"name":"values","kind":{"type":"array"},"nullable":false,"optional":false,"default":[],"indexed":false,"id_from_parent":null,"from":null,"app_type":"NestedValue","merge":null},{"name":"tree","kind":{"type":"object"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"NestedTree","merge":null,"fields":[{"name":"name","kind":{"type":"scalar","kind":"str"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"children","kind":{"type":"array"},"nullable":false,"optional":false,"default":[],"indexed":false,"id_from_parent":null,"from":null,"app_type":"NestedTree","merge":null}]},{"name":"replyTo","kind":{"type":"object"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"MessageReplyNode","merge":null,"fields":[{"name":"id","kind":{"type":"scalar","kind":"id"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"body","kind":{"type":"scalar","kind":"str"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"kind","kind":{"type":"scalar","kind":"str"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"userId","kind":{"type":"scalar","kind":"id"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"user","kind":{"type":"object"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"MessageUserRef","merge":null,"fields":[{"name":"id","kind":{"type":"scalar","kind":"id"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"fullName","kind":{"type":"scalar","kind":"str"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"name","kind":{"type":"scalar","kind":"str"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"username","kind":{"type":"scalar","kind":"str"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"avatarUrl","kind":{"type":"scalar","kind":"str"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null}]},{"name":"media","kind":{"type":"object"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"NestedMedia","merge":null,"fields":[{"name":"id","kind":{"type":"scalar","kind":"id"},"nullable":false,"optional":false,"default":"","indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"fileUrl","kind":{"type":"scalar","kind":"str"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null}]}]}],"associations":[],"relations":[],"composite_indexes":[],"actions":[],"ephemeral":[],"storage":"local","expire_after":null,"listens":[]},{"key":"Presence","fields":[{"name":"id","kind":{"type":"scalar","kind":"id"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"userId","kind":{"type":"scalar","kind":"id"},"nullable":false,"optional":false,"default":null,"indexed":true,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"room","kind":{"type":"scalar","kind":"str"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null}],"associations":[{"name":"user","kind":"belongs_to","target":"User","foreign_key":"userId","dependent":null,"touches_parent":false,"sets":{},"newer":[],"from":null}],"relations":[{"name":"for_user","by_columns":["userId"],"text_params":[],"remote":null,"is_global":false,"sort":"server_order","joins_events":false,"joins_created":false},{"name":"online","by_columns":[],"text_params":[],"remote":{"operation":"PresenceOnline","shape":{"type":"list","coverage":"complete"},"stale_time":"30.seconds","empty_stale_time":null,"selects_page_info":false,"updates":[{"subscription":"PresenceChanged","deleted_by":"left","set":{},"removes":false,"debounce":null,"root":null,"id":null}],"page_size":null,"variables":{},"root":null},"is_global":true,"sort":"server_order","joins_events":false,"joins_created":false}],"composite_indexes":[],"actions":[],"ephemeral":[],"storage":"memory","expire_after":"1.minute","listens":[]},{"key":"User","fields":[{"name":"id","kind":{"type":"scalar","kind":"id"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"name","kind":{"type":"scalar","kind":"str"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"avatar","kind":{"type":"file"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"birthday","kind":{"type":"scalar","kind":"date"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null}],"associations":[],"relations":[{"name":"profile","by_columns":[],"text_params":[],"remote":{"operation":"UserProfile","shape":{"type":"single"},"stale_time":"5.minutes","empty_stale_time":null,"selects_page_info":false,"updates":[{"subscription":"UserTyping","deleted_by":null,"set":{"typing":true},"removes":false,"debounce":null,"root":null,"id":null}],"page_size":null,"variables":{},"root":null},"is_global":true,"sort":"server_order","joins_events":false,"joins_created":false}],"composite_indexes":[],"actions":[],"ephemeral":[{"name":"typing","kind":{"type":"scalar","kind":"bool"},"nullable":false,"optional":false,"default":false,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null,"reset_after":"5.seconds"},{"name":"mood","kind":{"type":"scalar","kind":"str"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null,"reset_after":null}],"storage":"disk","expire_after":null,"listens":[]},{"key":"Vibe","fields":[{"name":"id","kind":{"type":"scalar","kind":"id"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"momentId","kind":{"type":"scalar","kind":"id"},"nullable":false,"optional":false,"default":null,"indexed":true,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"userId","kind":{"type":"scalar","kind":"id"},"nullable":false,"optional":false,"default":null,"indexed":true,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"kind","kind":{"type":"enum","values":["like","love","laugh"]},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null}],"associations":[],"relations":[],"composite_indexes":[],"actions":[],"ephemeral":[],"storage":"disk","expire_after":null,"listens":[]}]"#
