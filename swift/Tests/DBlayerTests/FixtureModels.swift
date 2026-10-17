import Foundation
import DBlayer

public struct Chat: DBModel {
    public static let modelName = "Chat"
    public let id: String
    public let title: String
    public let lastMessageAt: String?
    public let lastMessageId: String?
    public let shareUrl: String?
    public let isPublic: Bool?
    public let sync: SyncInfo
    public let upload: UploadInfo?

    public init(id: String, title: String, lastMessageAt: String? = nil, lastMessageId: String? = nil, shareUrl: String? = nil, isPublic: Bool? = nil, sync: SyncInfo = SyncInfo(), upload: UploadInfo? = nil) {
        self.id = id
        self.title = title
        self.lastMessageAt = lastMessageAt
        self.lastMessageId = lastMessageId
        self.shareUrl = shareUrl
        self.isPublic = isPublic
        self.sync = sync
        self.upload = upload
    }

    public static let wireLayout = "9a2bed4e896c382db775b230c1ca95bd8142cfc7852a35c270abc4cbb3a6b541"

    public init(wire row: WireRow) throws {
        let r = CellReader(row.cells)
        self.id = row.id
        self.title = try r.req(0, \.asText)
        self.lastMessageAt = try r.opt(1, \.asText)
        self.lastMessageId = try r.opt(2, \.asId)
        self.shareUrl = try r.opt(3, \.asText)
        self.isPublic = try r.opt(4, \.asBool)
        self.sync = SyncInfo(wire: row.sync)
        self.upload = row.upload.map(UploadInfo.init(wire:))
    }

    public static let title = Column<Chat, String>("title")
    public static let lastMessageAt = Column<Chat, String?>("lastMessageAt")
    public static let lastMessageId = Column<Chat, String?>("lastMessageId")
    public static let shareUrl = Column<Chat, String?>("shareUrl")
    public static let isPublic = Column<Chat, Bool?>("isPublic")
    public static func find(_ id: String) -> LiveSource<Self?> { Query<Self>().find(id) }
    public static func `where`(_ condition: Condition) -> Query<Self> { Query<Self>().where(condition) }
    public static func node(_ id: String, force: Bool = false) async throws -> Self? {
        try await readNode(relation: "node", id: id, force: force)
    }
    public static func nodes(_ ids: [String]) async throws -> [Self] {
        try await readNodes(relation: "nodes", ids: ids)
    }
    public static var recent: Query<Self> {
        var built = Query<Self>(relation: "recent", params: [:])
        built = built.order(Order(field: "lastMessageAt", direction: "desc"))
        return built
    }
    public static func create(title: String, lastMessageAt: String? = nil, lastMessageId: String? = nil, shareUrl: String? = nil, isPublic: Bool? = nil) async throws -> Self {
        var values: [String: JSONValue] = [:]
        values["title"] = JSONValue.encode(title)
        if let value = lastMessageAt { values["lastMessageAt"] = JSONValue.encode(value) }
        if let value = lastMessageId { values["lastMessageId"] = JSONValue.encode(value) }
        if let value = shareUrl { values["shareUrl"] = JSONValue.encode(value) }
        if let value = isPublic { values["isPublic"] = JSONValue.encode(value) }
        return try await create(values: values)
    }
    public func update(title: String? = nil, lastMessageAt: String?? = nil, lastMessageId: String?? = nil, shareUrl: String?? = nil, isPublic: Bool?? = nil) async throws -> Self {
        var values: [String: JSONValue] = [:]
        if let value = title { values["title"] = JSONValue.encode(value) }
        if let value = lastMessageAt { values["lastMessageAt"] = JSONValue.encode(value) }
        if let value = lastMessageId { values["lastMessageId"] = JSONValue.encode(value) }
        if let value = shareUrl { values["shareUrl"] = JSONValue.encode(value) }
        if let value = isPublic { values["isPublic"] = JSONValue.encode(value) }
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
    public let sequenceNumber: Int?
    public let attachment: MessageAttachment?
    public let media: MessageMedia?
    public let sync: SyncInfo
    public let upload: UploadInfo?

    public init(id: String, chatId: String, authorId: String, text: String, createdAt: String, sequenceNumber: Int? = nil, attachment: MessageAttachment? = nil, media: MessageMedia? = nil, sync: SyncInfo = SyncInfo(), upload: UploadInfo? = nil) {
        self.id = id
        self.chatId = chatId
        self.authorId = authorId
        self.text = text
        self.createdAt = createdAt
        self.sequenceNumber = sequenceNumber
        self.attachment = attachment
        self.media = media
        self.sync = sync
        self.upload = upload
    }

    public static let wireLayout = "8b789705ea70aee5243f555f1e6f2efd5c8f00d31f63826d8db1cf73fc4fb31e"

    public init(wire row: WireRow) throws {
        let r = CellReader(row.cells)
        self.id = row.id
        self.chatId = try r.req(0, \.asId)
        self.authorId = try r.req(1, \.asId)
        self.text = try r.req(2, \.asText)
        self.createdAt = try r.req(3, \.asText)
        self.sequenceNumber = try r.opt(4, \.asInt)
        self.attachment = try r.objOpt(5, MessageAttachment.init(cells:))
        self.media = try r.objOpt(6, MessageMedia.init(cells:))
        self.sync = SyncInfo(wire: row.sync)
        self.upload = row.upload.map(UploadInfo.init(wire:))
    }

    public static let chatId = Column<Message, String>("chatId")
    public static let authorId = Column<Message, String>("authorId")
    public static let text = Column<Message, String>("text")
    public static let createdAt = Column<Message, String>("createdAt")
    public static let sequenceNumber = Column<Message, Int?>("sequenceNumber")
    public static let attachment = Column<Message, MessageAttachmentKind>("attachment")
    public static let media = Column<Message, MessageMedia?>("media")
    public static func find(_ id: String) -> LiveSource<Self?> { Query<Self>().find(id) }
    public static func `where`(_ condition: Condition) -> Query<Self> { Query<Self>().where(condition) }
    public static func byChat(_ chatId: String) -> Query<Self> {
        var built = Query<Self>(relation: "by_chat", params: ["chatId": JSONValue.encode(chatId)])
        built = built.where(Condition.equal("chatId", JSONValue.encode(chatId)))
        built = built.order(Order(field: "createdAt", direction: "asc"))
        return built
    }
    public static func thread(_ chatId: String) -> Query<Self> {
        let built = Query<Self>(relation: "thread", params: ["chatId": JSONValue.encode(chatId)])
        return built
    }
    public static func transcodeStatus(_ id: String, force: Bool = false) async throws -> Self? {
        try await readNode(relation: "transcode_status", id: id, force: force)
    }
    public static func chatMedia(_ chatId: String) -> Query<Self> {
        let built = Query<Self>(relation: "chat_media", params: ["chatId": JSONValue.encode(chatId)])
        return built
    }
    public static var history: Query<Self> {
        let built = Query<Self>(relation: "history", params: [:])
        return built
    }
    public static func create(chatId: String, authorId: String, text: String, attachedUserId: String? = nil, signedId: String? = nil) async throws -> Self {
        var values: [String: JSONValue] = [:]
        var args: [String: JSONValue] = [:]
        values["chatId"] = JSONValue.encode(chatId)
        values["authorId"] = JSONValue.encode(authorId)
        values["text"] = JSONValue.encode(text)
        if let value = attachedUserId { args["attachedUserId"] = JSONValue.encode(value) }
        if let value = signedId { args["signedId"] = JSONValue.encode(value) }
        return try await create(values: values, args: args)
    }
    public func update(chatId: String? = nil, authorId: String? = nil, text: String? = nil, createdAt: String? = nil, sequenceNumber: Int?? = nil, attachment: MessageAttachment?? = nil, media: MessageMedia?? = nil) async throws -> Self {
        var values: [String: JSONValue] = [:]
        if let value = chatId { values["chatId"] = JSONValue.encode(value) }
        if let value = authorId { values["authorId"] = JSONValue.encode(value) }
        if let value = text { values["text"] = JSONValue.encode(value) }
        if let value = createdAt { values["createdAt"] = JSONValue.encode(value) }
        if let value = sequenceNumber { values["sequenceNumber"] = JSONValue.encode(value) }
        if let value = attachment { values["attachment"] = JSONValue.encode(value) }
        if let value = media { values["media"] = JSONValue.encode(value) }
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

    public static let wireLayout = "1b66c84852ce5a61eeef6f7c5acf272acbdc9e265628ebcf4f9429e7f94772b1"

    public init(wire row: WireRow) throws {
        let r = CellReader(row.cells)
        self.id = row.id
        self.uuid = try r.req(0, \.asText)
        self.userId = try r.opt(1, \.asId)
        self.text = try r.opt(2, \.asText)
        self.likedByMe = try r.opt(3, \.asBool) ?? false
        self.media = try r.jsonOpt(4) ?? .object([:])
        self.tone = try r.opt(5, \.asText)
        self.createdAt = try r.req(6, \.asText)
        self.sync = SyncInfo(wire: row.sync)
        self.upload = row.upload.map(UploadInfo.init(wire:))
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
    public static func transcodeStatus(_ id: String, force: Bool = false) async throws -> Self? {
        try await readNode(relation: "transcode_status", id: id, force: force)
    }
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

public struct NestedSample: DBModel {
    public static let modelName = "NestedSample"
    public let id: String
    public let phase: NestedSamplePhase?
    public let value: NestedValue?
    public let values: [NestedValue]
    public let tree: NestedTree?
    public let replyTo: MessageReplyNode?
    public let sync: SyncInfo
    public let upload: UploadInfo?

    public init(id: String, phase: NestedSamplePhase? = nil, value: NestedValue? = nil, values: [NestedValue], tree: NestedTree? = nil, replyTo: MessageReplyNode? = nil, sync: SyncInfo = SyncInfo(), upload: UploadInfo? = nil) {
        self.id = id
        self.phase = phase
        self.value = value
        self.values = values
        self.tree = tree
        self.replyTo = replyTo
        self.sync = sync
        self.upload = upload
    }

    public static let wireLayout = "b98341f178193869c1bf875ecf8a6d93ab3789eaec78d978ad679f5ac20f46ca"

    public init(wire row: WireRow) throws {
        let r = CellReader(row.cells)
        self.id = row.id
        self.phase = try r.opt(0, \.asText).map(NestedSamplePhase.init(rawValue:))
        self.value = try r.objOpt(1, NestedValue.init(cells:))
        self.values = try r.arrOpt(2, NestedValue.init(cells:)) ?? []
        self.tree = try r.objOpt(3, NestedTree.init(cells:))
        self.replyTo = try r.objOpt(4, MessageReplyNode.init(cells:))
        self.sync = SyncInfo(wire: row.sync)
        self.upload = row.upload.map(UploadInfo.init(wire:))
    }

    public static let phase = Column<NestedSample, NestedSamplePhase?>("phase")
    public static let value = Column<NestedSample, NestedValue?>("value")
    public static let values = Column<NestedSample, [NestedValue]>("values")
    public static let tree = Column<NestedSample, NestedTree?>("tree")
    public static let replyTo = Column<NestedSample, MessageReplyNode?>("replyTo")
    public static func find(_ id: String) -> LiveSource<Self?> { Query<Self>().find(id) }
    public static func `where`(_ condition: Condition) -> Query<Self> { Query<Self>().where(condition) }
    public static func create(phase: NestedSamplePhase? = nil, value: NestedValue? = nil, values: [NestedValue]? = nil, tree: NestedTree? = nil, replyTo: MessageReplyNode? = nil) async throws -> Self {
        var values_: [String: JSONValue] = [:]
        if let value = phase { values_["phase"] = JSONValue.encode(value) }
        if let value = value { values_["value"] = JSONValue.encode(value) }
        if let value = values { values_["values"] = JSONValue.encode(value) }
        if let value = tree { values_["tree"] = JSONValue.encode(value) }
        if let value = replyTo { values_["replyTo"] = JSONValue.encode(value) }
        return try await create(values: values_)
    }
    public func update(phase: NestedSamplePhase?? = nil, value: NestedValue?? = nil, values: [NestedValue]? = nil, tree: NestedTree?? = nil, replyTo: MessageReplyNode?? = nil) async throws -> Self {
        var values_: [String: JSONValue] = [:]
        if let value = phase { values_["phase"] = JSONValue.encode(value) }
        if let value = value { values_["value"] = JSONValue.encode(value) }
        if let value = values { values_["values"] = JSONValue.encode(value) }
        if let value = tree { values_["tree"] = JSONValue.encode(value) }
        if let value = replyTo { values_["replyTo"] = JSONValue.encode(value) }
        return try await update(values: values_)
    }
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

    public static let wireLayout = "1f93bebc94e76afff074fd047e84d5066b4d70676ff703781e6eec12866579e4"

    public init(wire row: WireRow) throws {
        let r = CellReader(row.cells)
        self.id = row.id
        self.values = try r.arrOpt(0, NestedValue.init(cells:)) ?? []
        self.sync = SyncInfo(wire: row.sync)
        self.upload = row.upload.map(UploadInfo.init(wire:))
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

    public static let wireLayout = "6389b939e12075c3b9a36a258c0ebb41dee76a6ce786ccf5d21f28dad18bfc97"

    public init(wire row: WireRow) throws {
        let r = CellReader(row.cells)
        self.id = row.id
        self.userId = try r.req(0, \.asId)
        self.room = try r.req(1, \.asText)
        self.sync = SyncInfo(wire: row.sync)
        self.upload = row.upload.map(UploadInfo.init(wire:))
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

public struct CustomScreen: DBModel {
    public static let modelName = "CustomScreen"
    public let id: String
    public let slug: String
    public let spec: ScreenSpec?
    public let sync: SyncInfo
    public let upload: UploadInfo?

    public init(id: String, slug: String, spec: ScreenSpec? = nil, sync: SyncInfo = SyncInfo(), upload: UploadInfo? = nil) {
        self.id = id
        self.slug = slug
        self.spec = spec
        self.sync = sync
        self.upload = upload
    }

    public static let wireLayout = "2ee8e83236c749b2249aed88873cc60db6064cf630c886ed538336525207ffbc"

    public init(wire row: WireRow) throws {
        let r = CellReader(row.cells)
        self.id = row.id
        self.slug = try r.req(0, \.asText)
        self.spec = try r.objOpt(1, ScreenSpec.init(cells:))
        self.sync = SyncInfo(wire: row.sync)
        self.upload = row.upload.map(UploadInfo.init(wire:))
    }

    public static let slug = Column<CustomScreen, String>("slug")
    public static let spec = Column<CustomScreen, ScreenSpec?>("spec")
    public static func find(_ id: String) -> LiveSource<Self?> { Query<Self>().find(id) }
    public static func `where`(_ condition: Condition) -> Query<Self> { Query<Self>().where(condition) }
    public static func node(_ id: String, force: Bool = false) async throws -> Self? {
        try await readNode(relation: "node", id: id, force: force)
    }
    public static func create(slug: String, spec: ScreenSpec? = nil) async throws -> Self {
        var values: [String: JSONValue] = [:]
        values["slug"] = JSONValue.encode(slug)
        if let value = spec { values["spec"] = JSONValue.encode(value) }
        return try await create(values: values)
    }
    public func update(slug: String? = nil, spec: ScreenSpec?? = nil) async throws -> Self {
        var values: [String: JSONValue] = [:]
        if let value = slug { values["slug"] = JSONValue.encode(value) }
        if let value = spec { values["spec"] = JSONValue.encode(value) }
        return try await update(values: values)
    }
}

public struct StorageRow: DBModel {
    public static let modelName = "StorageRow"
    public let id: String
    public let record: String
    public let sync: SyncInfo
    public let upload: UploadInfo?

    public init(id: String, record: String, sync: SyncInfo = SyncInfo(), upload: UploadInfo? = nil) {
        self.id = id
        self.record = record
        self.sync = sync
        self.upload = upload
    }

    public static let wireLayout = "fe5fbb4f29b1efa251b84c3acf30c1c33a863db299f28340d13b798cba3d174f"

    public init(wire row: WireRow) throws {
        let r = CellReader(row.cells)
        self.id = row.id
        self.record = try r.req(0, \.asText)
        self.sync = SyncInfo(wire: row.sync)
        self.upload = row.upload.map(UploadInfo.init(wire:))
    }

    public static let record = Column<StorageRow, String>("record")
    public static func find(_ id: String) -> LiveSource<Self?> { Query<Self>().find(id) }
    public static func `where`(_ condition: Condition) -> Query<Self> { Query<Self>().where(condition) }
    public static func create(record: String) async throws -> Self {
        var values: [String: JSONValue] = [:]
        values["record"] = JSONValue.encode(record)
        return try await create(values: values)
    }
    public func update(record: String? = nil) async throws -> Self {
        var values: [String: JSONValue] = [:]
        if let value = record { values["record"] = JSONValue.encode(value) }
        return try await update(values: values)
    }
}

public struct UnionSample: DBModel {
    public static let modelName = "UnionSample"
    public let id: String
    public let label: String?
    public let attachment: MessageAttachment?
    public let props: MomentWidgetItemProps?
    public let sync: SyncInfo
    public let upload: UploadInfo?

    public init(id: String, label: String? = nil, attachment: MessageAttachment? = nil, props: MomentWidgetItemProps? = nil, sync: SyncInfo = SyncInfo(), upload: UploadInfo? = nil) {
        self.id = id
        self.label = label
        self.attachment = attachment
        self.props = props
        self.sync = sync
        self.upload = upload
    }

    public static let wireLayout = "1ee46c4fef7a5114130959afff43237741233a8211889420ea108ba15052bd58"

    public init(wire row: WireRow) throws {
        let r = CellReader(row.cells)
        self.id = row.id
        self.label = try r.opt(0, \.asText)
        self.attachment = try r.objOpt(1, MessageAttachment.init(cells:))
        self.props = try r.objOpt(2, MomentWidgetItemProps.init(cells:))
        self.sync = SyncInfo(wire: row.sync)
        self.upload = row.upload.map(UploadInfo.init(wire:))
    }

    public static let label = Column<UnionSample, String?>("label")
    public static let attachment = Column<UnionSample, MessageAttachmentKind>("attachment")
    public static let props = Column<UnionSample, MomentWidgetItemPropsKind>("props")
    public static func find(_ id: String) -> LiveSource<Self?> { Query<Self>().find(id) }
    public static func `where`(_ condition: Condition) -> Query<Self> { Query<Self>().where(condition) }
    public static func create(label: String? = nil, attachment: MessageAttachment? = nil, props: MomentWidgetItemProps? = nil) async throws -> Self {
        var values: [String: JSONValue] = [:]
        if let value = label { values["label"] = JSONValue.encode(value) }
        if let value = attachment { values["attachment"] = JSONValue.encode(value) }
        if let value = props { values["props"] = JSONValue.encode(value) }
        return try await create(values: values)
    }
    public func update(label: String?? = nil, attachment: MessageAttachment?? = nil, props: MomentWidgetItemProps?? = nil) async throws -> Self {
        var values: [String: JSONValue] = [:]
        if let value = label { values["label"] = JSONValue.encode(value) }
        if let value = attachment { values["attachment"] = JSONValue.encode(value) }
        if let value = props { values["props"] = JSONValue.encode(value) }
        return try await update(values: values)
    }
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

    public static let wireLayout = "4849bf8a3175334b01f39299c6e9512f671e8acabd8119121c8cc168047fb390"

    public init(wire row: WireRow) throws {
        let r = CellReader(row.cells)
        self.id = row.id
        self.name = try r.req(0, \.asText)
        self.avatar = try r.jsonOpt(1)
        self.birthday = try r.opt(2, \.asText)
        self.typing = try r.opt(3, \.asBool) ?? false
        self.mood = try r.opt(4, \.asText)
        self.sync = SyncInfo(wire: row.sync)
        self.upload = row.upload.map(UploadInfo.init(wire:))
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
    public let kind: VibeKind
    public let sync: SyncInfo
    public let upload: UploadInfo?

    public init(id: String, momentId: String, userId: String, kind: VibeKind, sync: SyncInfo = SyncInfo(), upload: UploadInfo? = nil) {
        self.id = id
        self.momentId = momentId
        self.userId = userId
        self.kind = kind
        self.sync = sync
        self.upload = upload
    }

    public static let wireLayout = "091ba73dd97a9be52544a900fa4ddd6515d3e70202d2fb1eb080d7211c9610b0"

    public init(wire row: WireRow) throws {
        let r = CellReader(row.cells)
        self.id = row.id
        self.momentId = try r.req(0, \.asId)
        self.userId = try r.req(1, \.asId)
        self.kind = try VibeKind(rawValue: r.req(2, \.asText))
        self.sync = SyncInfo(wire: row.sync)
        self.upload = row.upload.map(UploadInfo.init(wire:))
    }

    public static let momentId = Column<Vibe, String>("momentId")
    public static let userId = Column<Vibe, String>("userId")
    public static let kind = Column<Vibe, VibeKind>("kind")
    public static func find(_ id: String) -> LiveSource<Self?> { Query<Self>().find(id) }
    public static func `where`(_ condition: Condition) -> Query<Self> { Query<Self>().where(condition) }
    public static func create(momentId: String, userId: String, kind: VibeKind) async throws -> Self {
        var values: [String: JSONValue] = [:]
        values["momentId"] = JSONValue.encode(momentId)
        values["userId"] = JSONValue.encode(userId)
        values["kind"] = JSONValue.encode(kind)
        return try await create(values: values)
    }
    public func update(momentId: String? = nil, userId: String? = nil, kind: VibeKind? = nil) async throws -> Self {
        var values: [String: JSONValue] = [:]
        if let value = momentId { values["momentId"] = JSONValue.encode(value) }
        if let value = userId { values["userId"] = JSONValue.encode(value) }
        if let value = kind { values["kind"] = JSONValue.encode(value) }
        return try await update(values: values)
    }
}

public enum MessageAttachment: Codable, Sendable, Equatable {
    case gift(GiftAttachment)
    case moment(MomentAttachment)
    case profile(ProfileAttachment)
    case reaction(ReactionAttachment)
    case unknown(typename: String)

    public init(from decoder: Decoder) throws {
        let typename = try decoder.container(keyedBy: UnionTypenameKey.self).decode(String.self, forKey: .typename)
        switch typename {
        case "GiftAttachment": self = .gift(try GiftAttachment(from: decoder))
        case "MomentAttachment": self = .moment(try MomentAttachment(from: decoder))
        case "ProfileAttachment": self = .profile(try ProfileAttachment(from: decoder))
        case "ReactionAttachment": self = .reaction(try ReactionAttachment(from: decoder))
        default: self = .unknown(typename: typename)
        }
    }

    public func encode(to encoder: Encoder) throws {
        switch self {
        case let .gift(value): try JSONValue.tagged("GiftAttachment", value).encode(to: encoder)
        case let .moment(value): try JSONValue.tagged("MomentAttachment", value).encode(to: encoder)
        case let .profile(value): try JSONValue.tagged("ProfileAttachment", value).encode(to: encoder)
        case let .reaction(value): try JSONValue.tagged("ReactionAttachment", value).encode(to: encoder)
        case let .unknown(typename): try JSONValue.tagged(typename, [String: String]()).encode(to: encoder)
        }
    }

    public init(cells: [Cell]) throws {
        let r = CellReader(cells)
        let typename = try r.req(0, \.asText)
        switch typename {
        case "GiftAttachment": self = .gift(try r.obj(1, GiftAttachment.init(cells:)))
        case "MomentAttachment": self = .moment(try r.obj(1, MomentAttachment.init(cells:)))
        case "ProfileAttachment": self = .profile(try r.obj(1, ProfileAttachment.init(cells:)))
        case "ReactionAttachment": self = .reaction(try r.obj(1, ReactionAttachment.init(cells:)))
        default: self = .unknown(typename: typename)
        }
    }

    public var kind: MessageAttachmentKind? {
        switch self {
        case .gift: return .gift
        case .moment: return .moment
        case .profile: return .profile
        case .reaction: return .reaction
        case .unknown: return nil
        }
    }
}

public enum MessageAttachmentKind: String, Codable, Sendable {
    case gift = "GiftAttachment"
    case moment = "MomentAttachment"
    case profile = "ProfileAttachment"
    case reaction = "ReactionAttachment"
}

public struct GiftAttachment: Codable, Sendable, Equatable {
    public let id: String
    public let tier: String
    public let status: String?
    public let tags: [String]

    public init(id: String, tier: String, status: String? = nil, tags: [String] = []) {
        self.id = id
        self.tier = tier
        self.status = status
        self.tags = tags
    }

    private enum CodingKeys: String, CodingKey {
        case id = "id"
        case tier = "giftTier"
        case status = "status"
        case tags = "tags"
    }

    public init(from decoder: Decoder) throws {
        let container = try decoder.container(keyedBy: CodingKeys.self)
        id = try container.decode(IDText.self, forKey: .id).value
        tier = try container.decode(String.self, forKey: .tier)
        status = try container.decodeIfPresent(String.self, forKey: .status)
        tags = try container.decodeIfPresent([String].self, forKey: .tags) ?? []
    }

    public init(cells: [Cell]) throws {
        let r = CellReader(cells)
        id = try r.req(0, \.asId)
        tier = try r.req(1, \.asText)
        status = try r.opt(2, \.asText)
        tags = try r.listOpt(3, \.asText) ?? []
    }
}

public struct MomentAttachment: Codable, Sendable, Equatable {
    public let momentId: String
    public let caption: String?

    public init(momentId: String, caption: String? = nil) {
        self.momentId = momentId
        self.caption = caption
    }

    private enum CodingKeys: String, CodingKey {
        case momentId = "momentId"
        case caption = "caption"
    }

    public init(from decoder: Decoder) throws {
        let container = try decoder.container(keyedBy: CodingKeys.self)
        momentId = try container.decode(IDText.self, forKey: .momentId).value
        caption = try container.decodeIfPresent(String.self, forKey: .caption)
    }

    public init(cells: [Cell]) throws {
        let r = CellReader(cells)
        momentId = try r.req(0, \.asId)
        caption = try r.opt(1, \.asText)
    }
}

public struct ProfileAttachment: Codable, Sendable, Equatable {
    public let user: AttachmentUser?

    public init(user: AttachmentUser? = nil) {
        self.user = user
    }

    private enum CodingKeys: String, CodingKey {
        case user = "user"
    }

    public init(from decoder: Decoder) throws {
        let container = try decoder.container(keyedBy: CodingKeys.self)
        user = try container.decodeIfPresent(AttachmentUser.self, forKey: .user)
    }

    public init(cells: [Cell]) throws {
        let r = CellReader(cells)
        user = try r.objOpt(0, AttachmentUser.init(cells:))
    }
}

public struct AttachmentUser: Codable, Sendable, Equatable {
    public let id: String
    public let name: String?

    public init(id: String, name: String? = nil) {
        self.id = id
        self.name = name
    }

    private enum CodingKeys: String, CodingKey {
        case id = "id"
        case name = "name"
    }

    public init(from decoder: Decoder) throws {
        let container = try decoder.container(keyedBy: CodingKeys.self)
        id = try container.decode(IDText.self, forKey: .id).value
        name = try container.decodeIfPresent(String.self, forKey: .name)
    }

    public init(cells: [Cell]) throws {
        let r = CellReader(cells)
        id = try r.req(0, \.asId)
        name = try r.opt(1, \.asText)
    }
}

public struct ReactionAttachment: Codable, Sendable, Equatable {
    public let body: String

    public init(body: String) {
        self.body = body
    }

    private enum CodingKeys: String, CodingKey {
        case body = "body"
    }

    public init(from decoder: Decoder) throws {
        let container = try decoder.container(keyedBy: CodingKeys.self)
        body = try container.decode(String.self, forKey: .body)
    }

    public init(cells: [Cell]) throws {
        let r = CellReader(cells)
        body = try r.req(0, \.asText)
    }
}

public struct MessageMedia: Codable, Sendable, Equatable {
    public let kind: String?
    public let transcoded: Bool?
    public let fileUrl: String?

    public init(kind: String? = nil, transcoded: Bool? = nil, fileUrl: String? = nil) {
        self.kind = kind
        self.transcoded = transcoded
        self.fileUrl = fileUrl
    }

    private enum CodingKeys: String, CodingKey {
        case kind = "kind"
        case transcoded = "transcoded"
        case fileUrl = "fileUrl"
    }

    public init(from decoder: Decoder) throws {
        let container = try decoder.container(keyedBy: CodingKeys.self)
        kind = try container.decodeIfPresent(String.self, forKey: .kind)
        transcoded = try container.decodeIfPresent(Bool.self, forKey: .transcoded)
        fileUrl = try container.decodeIfPresent(String.self, forKey: .fileUrl)
    }

    public init(cells: [Cell]) throws {
        let r = CellReader(cells)
        kind = try r.opt(0, \.asText)
        transcoded = try r.opt(1, \.asBool)
        fileUrl = try r.opt(2, \.asText)
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
    public let state: NestedValueState
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

    public init(title: String, note: String? = nil, nullableDefault: String? = "default", ownerId: String? = nil, count: Int = 2, ratio: Double = 1.5, enabled: Bool = false, createdAt: String? = nil, state: NestedValueState = NestedValueState.ready, labels: [String] = [], counts: [Int] = [], ratios: [Double] = [], flags: [Bool] = [], dates: [String] = [], owners: [String] = [], payload: JSONValue? = nil, rawValue: JSONValue = .object(["free": .array([.number(1), .bool(true), .null])]), preferences: NestedValuePreferences = NestedValuePreferences(label: "base"), presets: [NestedValuePreferences] = [NestedValuePreferences(label: "first")], metadata: JSONValue? = nil, values: JSONValue? = nil, child: NestedValueChild? = nil, children: [NestedValueChild] = []) {
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
        state = try container.decodeIfPresent(NestedValueState.self, forKey: .state) ?? NestedValueState.ready
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

    public init(cells: [Cell]) throws {
        let r = CellReader(cells)
        title = try r.req(0, \.asText)
        note = try r.opt(1, \.asText)
        nullableDefault = try r.opt(2, \.asText) ?? "default"
        ownerId = try r.opt(3, \.asId)
        count = try r.opt(4, \.asInt) ?? 2
        ratio = try r.opt(5, \.asDouble) ?? 1.5
        enabled = try r.opt(6, \.asBool) ?? false
        createdAt = try r.opt(7, \.asText)
        state = try r.opt(8, \.asText).map(NestedValueState.init(rawValue:)) ?? NestedValueState.ready
        labels = try r.listOpt(9, \.asText) ?? []
        counts = try r.listOpt(10, \.asInt) ?? []
        ratios = try r.listOpt(11, \.asDouble) ?? []
        flags = try r.listOpt(12, \.asBool) ?? []
        dates = try r.listOpt(13, \.asText) ?? []
        owners = try r.listOpt(14, \.asId) ?? []
        payload = try r.jsonOpt(15)
        rawValue = try r.jsonOpt(16) ?? .object(["free": .array([.number(1), .bool(true), .null])])
        preferences = try r.objOpt(17, NestedValuePreferences.init(cells:)) ?? NestedValuePreferences(label: "base")
        presets = try r.arrOpt(18, NestedValuePreferences.init(cells:)) ?? [NestedValuePreferences(label: "first")]
        metadata = try r.jsonOpt(19)
        values = try r.jsonOpt(20)
        child = try r.objOpt(21, NestedValueChild.init(cells:))
        children = try r.arrOpt(22, NestedValueChild.init(cells:)) ?? []
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

    public init(cells: [Cell]) throws {
        let r = CellReader(cells)
        label = try r.req(0, \.asText)
        active = try r.opt(1, \.asBool) ?? true
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

    public init(cells: [Cell]) throws {
        let r = CellReader(cells)
        body = try r.opt(0, \.asText)
        horizontal = try r.opt(1, \.asDouble) ?? 0.0
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

    public init(cells: [Cell]) throws {
        let r = CellReader(cells)
        name = try r.req(0, \.asText)
        children = try r.arrOpt(1, NestedTree.init(cells:)) ?? []
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

    public init(cells: [Cell]) throws {
        let r = CellReader(cells)
        id = try r.opt(0, \.asId)
        body = try r.opt(1, \.asText)
        kind = try r.opt(2, \.asText)
        userId = try r.opt(3, \.asId)
        user = try r.objOpt(4, MessageUserRef.init(cells:))
        media = try r.objOpt(5, NestedMedia.init(cells:))
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

    public init(cells: [Cell]) throws {
        let r = CellReader(cells)
        id = try r.opt(0, \.asId)
        fullName = try r.opt(1, \.asText)
        name = try r.opt(2, \.asText)
        username = try r.opt(3, \.asText)
        avatarUrl = try r.opt(4, \.asText)
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

    public init(cells: [Cell]) throws {
        let r = CellReader(cells)
        id = try r.opt(0, \.asId) ?? ""
        fileUrl = try r.opt(1, \.asText)
    }
}

public struct ScreenSpec: Codable, Sendable, Equatable {
    public let root: String
    public let elements: [ScreenElement]
    public let state: [ScreenStateEntry]

    public init(root: String, elements: [ScreenElement], state: [ScreenStateEntry]) {
        self.root = root
        self.elements = elements
        self.state = state
    }

    private enum CodingKeys: String, CodingKey {
        case root = "root"
        case elements = "elements"
        case state = "state"
    }

    public init(from decoder: Decoder) throws {
        let container = try decoder.container(keyedBy: CodingKeys.self)
        root = try container.decode(IDText.self, forKey: .root).value
        elements = try container.decode([ScreenElement].self, forKey: .elements)
        state = try container.decode([ScreenStateEntry].self, forKey: .state)
    }

    public init(cells: [Cell]) throws {
        let r = CellReader(cells)
        root = try r.req(0, \.asId)
        elements = try r.arr(1, ScreenElement.init(cells:))
        state = try r.arr(2, ScreenStateEntry.init(cells:))
    }
}

public enum ScreenElement: Codable, Sendable, Equatable {
    case button(ScreenButton)
    case column(ScreenColumn)
    case heading(ScreenHeading)
    case pressable(ScreenPressable)
    case spacer(ScreenSpacer)
    case `switch`(ScreenSwitch)
    case unknown(typename: String)

    public init(from decoder: Decoder) throws {
        let typename = try decoder.container(keyedBy: UnionTypenameKey.self).decode(String.self, forKey: .typename)
        switch typename {
        case "ScreenButton": self = .button(try ScreenButton(from: decoder))
        case "ScreenColumn": self = .column(try ScreenColumn(from: decoder))
        case "ScreenHeading": self = .heading(try ScreenHeading(from: decoder))
        case "ScreenPressable": self = .pressable(try ScreenPressable(from: decoder))
        case "ScreenSpacer": self = .spacer(try ScreenSpacer(from: decoder))
        case "ScreenSwitch": self = .`switch`(try ScreenSwitch(from: decoder))
        default: self = .unknown(typename: typename)
        }
    }

    public func encode(to encoder: Encoder) throws {
        switch self {
        case let .button(value): try JSONValue.tagged("ScreenButton", value).encode(to: encoder)
        case let .column(value): try JSONValue.tagged("ScreenColumn", value).encode(to: encoder)
        case let .heading(value): try JSONValue.tagged("ScreenHeading", value).encode(to: encoder)
        case let .pressable(value): try JSONValue.tagged("ScreenPressable", value).encode(to: encoder)
        case let .spacer(value): try JSONValue.tagged("ScreenSpacer", value).encode(to: encoder)
        case let .`switch`(value): try JSONValue.tagged("ScreenSwitch", value).encode(to: encoder)
        case let .unknown(typename): try JSONValue.tagged(typename, [String: String]()).encode(to: encoder)
        }
    }

    public init(cells: [Cell]) throws {
        let r = CellReader(cells)
        let typename = try r.req(0, \.asText)
        switch typename {
        case "ScreenButton": self = .button(try r.obj(1, ScreenButton.init(cells:)))
        case "ScreenColumn": self = .column(try r.obj(1, ScreenColumn.init(cells:)))
        case "ScreenHeading": self = .heading(try r.obj(1, ScreenHeading.init(cells:)))
        case "ScreenPressable": self = .pressable(try r.obj(1, ScreenPressable.init(cells:)))
        case "ScreenSpacer": self = .spacer(try r.obj(1, ScreenSpacer.init(cells:)))
        case "ScreenSwitch": self = .`switch`(try r.obj(1, ScreenSwitch.init(cells:)))
        default: self = .unknown(typename: typename)
        }
    }

    public var id: String? {
        switch self {
        case let .button(value): return value.id
        case let .column(value): return value.id
        case let .heading(value): return value.id
        case let .pressable(value): return value.id
        case let .spacer(value): return value.id
        case let .`switch`(value): return value.id
        case .unknown: return nil
        }
    }

    public var children: [String]? {
        switch self {
        case let .button(value): return value.children
        case let .column(value): return value.children
        case let .heading(value): return value.children
        case let .pressable(value): return value.children
        case let .spacer(value): return value.children
        case let .`switch`(value): return value.children
        case .unknown: return nil
        }
    }

    public var press: ScreenAction? {
        switch self {
        case let .button(value): return value.press
        case let .column(value): return value.press
        case let .heading(value): return value.press
        case let .pressable(value): return value.press
        case let .spacer(value): return value.press
        case let .`switch`(value): return value.press
        case .unknown: return nil
        }
    }

    public var visible: ScreenVisibility? {
        switch self {
        case let .button(value): return value.visible
        case let .column(value): return value.visible
        case let .heading(value): return value.visible
        case let .pressable(value): return value.visible
        case let .spacer(value): return value.visible
        case let .`switch`(value): return value.visible
        case .unknown: return nil
        }
    }

    public var kind: ScreenElementKind? {
        switch self {
        case .button: return .button
        case .column: return .column
        case .heading: return .heading
        case .pressable: return .pressable
        case .spacer: return .spacer
        case .`switch`: return .`switch`
        case .unknown: return nil
        }
    }
}

public enum ScreenElementKind: String, Codable, Sendable {
    case button = "ScreenButton"
    case column = "ScreenColumn"
    case heading = "ScreenHeading"
    case pressable = "ScreenPressable"
    case spacer = "ScreenSpacer"
    case `switch` = "ScreenSwitch"
}

public struct ScreenButton: Codable, Sendable, Equatable {
    public let id: String
    public let children: [String]
    public let press: ScreenAction?
    public let visible: ScreenVisibility?
    public let label: ScreenTextValue?
    public let disabled: ScreenFlagValue?

    public init(id: String, children: [String], press: ScreenAction? = nil, visible: ScreenVisibility? = nil, label: ScreenTextValue? = nil, disabled: ScreenFlagValue? = nil) {
        self.id = id
        self.children = children
        self.press = press
        self.visible = visible
        self.label = label
        self.disabled = disabled
    }

    private enum CodingKeys: String, CodingKey {
        case id = "id"
        case children = "children"
        case press = "press"
        case visible = "visible"
        case label = "label"
        case disabled = "disabled"
    }

    public init(from decoder: Decoder) throws {
        let container = try decoder.container(keyedBy: CodingKeys.self)
        id = try container.decode(IDText.self, forKey: .id).value
        children = try container.decode([IDText].self, forKey: .children).map(\.value)
        press = try container.decodeIfPresent(ScreenAction.self, forKey: .press)
        visible = try container.decodeIfPresent(ScreenVisibility.self, forKey: .visible)
        label = try container.decodeIfPresent(ScreenTextValue.self, forKey: .label)
        disabled = try container.decodeIfPresent(ScreenFlagValue.self, forKey: .disabled)
    }

    public init(cells: [Cell]) throws {
        let r = CellReader(cells)
        id = try r.req(0, \.asId)
        children = try r.list(1, \.asId)
        press = try r.objOpt(2, ScreenAction.init(cells:))
        visible = try r.objOpt(3, ScreenVisibility.init(cells:))
        label = try r.objOpt(4, ScreenTextValue.init(cells:))
        disabled = try r.objOpt(5, ScreenFlagValue.init(cells:))
    }
}

public enum ScreenAction: Codable, Sendable, Equatable {
    case navigate(ScreenNavigateAction)
    case openChat(ScreenOpenChatAction)
    case openExternal(ScreenOpenExternalAction)
    case openProfile(ScreenOpenProfileAction)
    case unknown(typename: String)

    public init(from decoder: Decoder) throws {
        let typename = try decoder.container(keyedBy: UnionTypenameKey.self).decode(String.self, forKey: .typename)
        switch typename {
        case "ScreenNavigateAction": self = .navigate(try ScreenNavigateAction(from: decoder))
        case "ScreenOpenChatAction": self = .openChat(try ScreenOpenChatAction(from: decoder))
        case "ScreenOpenExternalAction": self = .openExternal(try ScreenOpenExternalAction(from: decoder))
        case "ScreenOpenProfileAction": self = .openProfile(try ScreenOpenProfileAction(from: decoder))
        default: self = .unknown(typename: typename)
        }
    }

    public func encode(to encoder: Encoder) throws {
        switch self {
        case let .navigate(value): try JSONValue.tagged("ScreenNavigateAction", value).encode(to: encoder)
        case let .openChat(value): try JSONValue.tagged("ScreenOpenChatAction", value).encode(to: encoder)
        case let .openExternal(value): try JSONValue.tagged("ScreenOpenExternalAction", value).encode(to: encoder)
        case let .openProfile(value): try JSONValue.tagged("ScreenOpenProfileAction", value).encode(to: encoder)
        case let .unknown(typename): try JSONValue.tagged(typename, [String: String]()).encode(to: encoder)
        }
    }

    public init(cells: [Cell]) throws {
        let r = CellReader(cells)
        let typename = try r.req(0, \.asText)
        switch typename {
        case "ScreenNavigateAction": self = .navigate(try r.obj(1, ScreenNavigateAction.init(cells:)))
        case "ScreenOpenChatAction": self = .openChat(try r.obj(1, ScreenOpenChatAction.init(cells:)))
        case "ScreenOpenExternalAction": self = .openExternal(try r.obj(1, ScreenOpenExternalAction.init(cells:)))
        case "ScreenOpenProfileAction": self = .openProfile(try r.obj(1, ScreenOpenProfileAction.init(cells:)))
        default: self = .unknown(typename: typename)
        }
    }

    public var kind: ScreenActionKind? {
        switch self {
        case .navigate: return .navigate
        case .openChat: return .openChat
        case .openExternal: return .openExternal
        case .openProfile: return .openProfile
        case .unknown: return nil
        }
    }
}

public enum ScreenActionKind: String, Codable, Sendable {
    case navigate = "ScreenNavigateAction"
    case openChat = "ScreenOpenChatAction"
    case openExternal = "ScreenOpenExternalAction"
    case openProfile = "ScreenOpenProfileAction"
}

public struct ScreenNavigateAction: Codable, Sendable, Equatable {
    public let slug: String
    public let title: String?

    public init(slug: String, title: String? = nil) {
        self.slug = slug
        self.title = title
    }

    private enum CodingKeys: String, CodingKey {
        case slug = "slug"
        case title = "title"
    }

    public init(from decoder: Decoder) throws {
        let container = try decoder.container(keyedBy: CodingKeys.self)
        slug = try container.decode(String.self, forKey: .slug)
        title = try container.decodeIfPresent(String.self, forKey: .title)
    }

    public init(cells: [Cell]) throws {
        let r = CellReader(cells)
        slug = try r.req(0, \.asText)
        title = try r.opt(1, \.asText)
    }
}

public struct ScreenOpenChatAction: Codable, Sendable, Equatable {
    public let chatId: String

    public init(chatId: String) {
        self.chatId = chatId
    }

    private enum CodingKeys: String, CodingKey {
        case chatId = "chatId"
    }

    public init(from decoder: Decoder) throws {
        let container = try decoder.container(keyedBy: CodingKeys.self)
        chatId = try container.decode(String.self, forKey: .chatId)
    }

    public init(cells: [Cell]) throws {
        let r = CellReader(cells)
        chatId = try r.req(0, \.asText)
    }
}

public struct ScreenOpenExternalAction: Codable, Sendable, Equatable {
    public let url: String

    public init(url: String) {
        self.url = url
    }

    private enum CodingKeys: String, CodingKey {
        case url = "url"
    }

    public init(from decoder: Decoder) throws {
        let container = try decoder.container(keyedBy: CodingKeys.self)
        url = try container.decode(String.self, forKey: .url)
    }

    public init(cells: [Cell]) throws {
        let r = CellReader(cells)
        url = try r.req(0, \.asText)
    }
}

public struct ScreenOpenProfileAction: Codable, Sendable, Equatable {
    public let userId: String

    public init(userId: String) {
        self.userId = userId
    }

    private enum CodingKeys: String, CodingKey {
        case userId = "userId"
    }

    public init(from decoder: Decoder) throws {
        let container = try decoder.container(keyedBy: CodingKeys.self)
        userId = try container.decode(String.self, forKey: .userId)
    }

    public init(cells: [Cell]) throws {
        let r = CellReader(cells)
        userId = try r.req(0, \.asText)
    }
}

public enum ScreenVisibility: Codable, Sendable, Equatable {
    case condition(ScreenCondition)
    case visibleAll(ScreenVisibleAll)
    case visibleAny(ScreenVisibleAny)
    case visibleFlag(ScreenVisibleFlag)
    case unknown(typename: String)

    public init(from decoder: Decoder) throws {
        let typename = try decoder.container(keyedBy: UnionTypenameKey.self).decode(String.self, forKey: .typename)
        switch typename {
        case "ScreenCondition": self = .condition(try ScreenCondition(from: decoder))
        case "ScreenVisibleAll": self = .visibleAll(try ScreenVisibleAll(from: decoder))
        case "ScreenVisibleAny": self = .visibleAny(try ScreenVisibleAny(from: decoder))
        case "ScreenVisibleFlag": self = .visibleFlag(try ScreenVisibleFlag(from: decoder))
        default: self = .unknown(typename: typename)
        }
    }

    public func encode(to encoder: Encoder) throws {
        switch self {
        case let .condition(value): try JSONValue.tagged("ScreenCondition", value).encode(to: encoder)
        case let .visibleAll(value): try JSONValue.tagged("ScreenVisibleAll", value).encode(to: encoder)
        case let .visibleAny(value): try JSONValue.tagged("ScreenVisibleAny", value).encode(to: encoder)
        case let .visibleFlag(value): try JSONValue.tagged("ScreenVisibleFlag", value).encode(to: encoder)
        case let .unknown(typename): try JSONValue.tagged(typename, [String: String]()).encode(to: encoder)
        }
    }

    public init(cells: [Cell]) throws {
        let r = CellReader(cells)
        let typename = try r.req(0, \.asText)
        switch typename {
        case "ScreenCondition": self = .condition(try r.obj(1, ScreenCondition.init(cells:)))
        case "ScreenVisibleAll": self = .visibleAll(try r.obj(1, ScreenVisibleAll.init(cells:)))
        case "ScreenVisibleAny": self = .visibleAny(try r.obj(1, ScreenVisibleAny.init(cells:)))
        case "ScreenVisibleFlag": self = .visibleFlag(try r.obj(1, ScreenVisibleFlag.init(cells:)))
        default: self = .unknown(typename: typename)
        }
    }

    public var kind: ScreenVisibilityKind? {
        switch self {
        case .condition: return .condition
        case .visibleAll: return .visibleAll
        case .visibleAny: return .visibleAny
        case .visibleFlag: return .visibleFlag
        case .unknown: return nil
        }
    }
}

public enum ScreenVisibilityKind: String, Codable, Sendable {
    case condition = "ScreenCondition"
    case visibleAll = "ScreenVisibleAll"
    case visibleAny = "ScreenVisibleAny"
    case visibleFlag = "ScreenVisibleFlag"
}

public struct ScreenCondition: Codable, Sendable, Equatable {
    public let comparison: String
    public let negated: Bool
    public let operand: ScreenOperand?
    public let path: String

    public init(comparison: String, negated: Bool, operand: ScreenOperand? = nil, path: String) {
        self.comparison = comparison
        self.negated = negated
        self.operand = operand
        self.path = path
    }

    private enum CodingKeys: String, CodingKey {
        case comparison = "comparison"
        case negated = "negated"
        case operand = "operand"
        case path = "path"
    }

    public init(from decoder: Decoder) throws {
        let container = try decoder.container(keyedBy: CodingKeys.self)
        comparison = try container.decode(String.self, forKey: .comparison)
        negated = try container.decode(Bool.self, forKey: .negated)
        operand = try container.decodeIfPresent(ScreenOperand.self, forKey: .operand)
        path = try container.decode(String.self, forKey: .path)
    }

    public init(cells: [Cell]) throws {
        let r = CellReader(cells)
        comparison = try r.req(0, \.asText)
        negated = try r.req(1, \.asBool)
        operand = try r.objOpt(2, ScreenOperand.init(cells:))
        path = try r.req(3, \.asText)
    }
}

public enum ScreenOperand: Codable, Sendable, Equatable {
    case flagLiteral(ScreenFlagLiteral)
    case numberLiteral(ScreenNumberLiteral)
    case stateRef(ScreenStateRef)
    case textLiteral(ScreenTextLiteral)
    case unknown(typename: String)

    public init(from decoder: Decoder) throws {
        let typename = try decoder.container(keyedBy: UnionTypenameKey.self).decode(String.self, forKey: .typename)
        switch typename {
        case "ScreenFlagLiteral": self = .flagLiteral(try ScreenFlagLiteral(from: decoder))
        case "ScreenNumberLiteral": self = .numberLiteral(try ScreenNumberLiteral(from: decoder))
        case "ScreenStateRef": self = .stateRef(try ScreenStateRef(from: decoder))
        case "ScreenTextLiteral": self = .textLiteral(try ScreenTextLiteral(from: decoder))
        default: self = .unknown(typename: typename)
        }
    }

    public func encode(to encoder: Encoder) throws {
        switch self {
        case let .flagLiteral(value): try JSONValue.tagged("ScreenFlagLiteral", value).encode(to: encoder)
        case let .numberLiteral(value): try JSONValue.tagged("ScreenNumberLiteral", value).encode(to: encoder)
        case let .stateRef(value): try JSONValue.tagged("ScreenStateRef", value).encode(to: encoder)
        case let .textLiteral(value): try JSONValue.tagged("ScreenTextLiteral", value).encode(to: encoder)
        case let .unknown(typename): try JSONValue.tagged(typename, [String: String]()).encode(to: encoder)
        }
    }

    public init(cells: [Cell]) throws {
        let r = CellReader(cells)
        let typename = try r.req(0, \.asText)
        switch typename {
        case "ScreenFlagLiteral": self = .flagLiteral(try r.obj(1, ScreenFlagLiteral.init(cells:)))
        case "ScreenNumberLiteral": self = .numberLiteral(try r.obj(1, ScreenNumberLiteral.init(cells:)))
        case "ScreenStateRef": self = .stateRef(try r.obj(1, ScreenStateRef.init(cells:)))
        case "ScreenTextLiteral": self = .textLiteral(try r.obj(1, ScreenTextLiteral.init(cells:)))
        default: self = .unknown(typename: typename)
        }
    }

    public var kind: ScreenOperandKind? {
        switch self {
        case .flagLiteral: return .flagLiteral
        case .numberLiteral: return .numberLiteral
        case .stateRef: return .stateRef
        case .textLiteral: return .textLiteral
        case .unknown: return nil
        }
    }
}

public enum ScreenOperandKind: String, Codable, Sendable {
    case flagLiteral = "ScreenFlagLiteral"
    case numberLiteral = "ScreenNumberLiteral"
    case stateRef = "ScreenStateRef"
    case textLiteral = "ScreenTextLiteral"
}

public struct ScreenFlagLiteral: Codable, Sendable, Equatable {
    public let flag: Bool

    public init(flag: Bool) {
        self.flag = flag
    }

    private enum CodingKeys: String, CodingKey {
        case flag = "flag"
    }

    public init(from decoder: Decoder) throws {
        let container = try decoder.container(keyedBy: CodingKeys.self)
        flag = try container.decode(Bool.self, forKey: .flag)
    }

    public init(cells: [Cell]) throws {
        let r = CellReader(cells)
        flag = try r.req(0, \.asBool)
    }
}

public struct ScreenNumberLiteral: Codable, Sendable, Equatable {
    public let number: Double

    public init(number: Double) {
        self.number = number
    }

    private enum CodingKeys: String, CodingKey {
        case number = "number"
    }

    public init(from decoder: Decoder) throws {
        let container = try decoder.container(keyedBy: CodingKeys.self)
        number = try container.decode(Double.self, forKey: .number)
    }

    public init(cells: [Cell]) throws {
        let r = CellReader(cells)
        number = try r.req(0, \.asDouble)
    }
}

public struct ScreenStateRef: Codable, Sendable, Equatable {
    public let path: String

    public init(path: String) {
        self.path = path
    }

    private enum CodingKeys: String, CodingKey {
        case path = "path"
    }

    public init(from decoder: Decoder) throws {
        let container = try decoder.container(keyedBy: CodingKeys.self)
        path = try container.decode(String.self, forKey: .path)
    }

    public init(cells: [Cell]) throws {
        let r = CellReader(cells)
        path = try r.req(0, \.asText)
    }
}

public struct ScreenTextLiteral: Codable, Sendable, Equatable {
    public let text: String

    public init(text: String) {
        self.text = text
    }

    private enum CodingKeys: String, CodingKey {
        case text = "text"
    }

    public init(from decoder: Decoder) throws {
        let container = try decoder.container(keyedBy: CodingKeys.self)
        text = try container.decode(String.self, forKey: .text)
    }

    public init(cells: [Cell]) throws {
        let r = CellReader(cells)
        text = try r.req(0, \.asText)
    }
}

public struct ScreenVisibleAll: Codable, Sendable, Equatable {
    public let conditions: [ScreenCondition]

    public init(conditions: [ScreenCondition]) {
        self.conditions = conditions
    }

    private enum CodingKeys: String, CodingKey {
        case conditions = "conditions"
    }

    public init(from decoder: Decoder) throws {
        let container = try decoder.container(keyedBy: CodingKeys.self)
        conditions = try container.decode([ScreenCondition].self, forKey: .conditions)
    }

    public init(cells: [Cell]) throws {
        let r = CellReader(cells)
        conditions = try r.arr(0, ScreenCondition.init(cells:))
    }
}

public struct ScreenVisibleAny: Codable, Sendable, Equatable {
    public let conditions: [ScreenCondition]

    public init(conditions: [ScreenCondition]) {
        self.conditions = conditions
    }

    private enum CodingKeys: String, CodingKey {
        case conditions = "conditions"
    }

    public init(from decoder: Decoder) throws {
        let container = try decoder.container(keyedBy: CodingKeys.self)
        conditions = try container.decode([ScreenCondition].self, forKey: .conditions)
    }

    public init(cells: [Cell]) throws {
        let r = CellReader(cells)
        conditions = try r.arr(0, ScreenCondition.init(cells:))
    }
}

public struct ScreenVisibleFlag: Codable, Sendable, Equatable {
    public let flag: Bool

    public init(flag: Bool) {
        self.flag = flag
    }

    private enum CodingKeys: String, CodingKey {
        case flag = "flag"
    }

    public init(from decoder: Decoder) throws {
        let container = try decoder.container(keyedBy: CodingKeys.self)
        flag = try container.decode(Bool.self, forKey: .flag)
    }

    public init(cells: [Cell]) throws {
        let r = CellReader(cells)
        flag = try r.req(0, \.asBool)
    }
}

public enum ScreenTextValue: Codable, Sendable, Equatable {
    case stateRef(ScreenStateRef)
    case textLiteral(ScreenTextLiteral)
    case unknown(typename: String)

    public init(from decoder: Decoder) throws {
        let typename = try decoder.container(keyedBy: UnionTypenameKey.self).decode(String.self, forKey: .typename)
        switch typename {
        case "ScreenStateRef": self = .stateRef(try ScreenStateRef(from: decoder))
        case "ScreenTextLiteral": self = .textLiteral(try ScreenTextLiteral(from: decoder))
        default: self = .unknown(typename: typename)
        }
    }

    public func encode(to encoder: Encoder) throws {
        switch self {
        case let .stateRef(value): try JSONValue.tagged("ScreenStateRef", value).encode(to: encoder)
        case let .textLiteral(value): try JSONValue.tagged("ScreenTextLiteral", value).encode(to: encoder)
        case let .unknown(typename): try JSONValue.tagged(typename, [String: String]()).encode(to: encoder)
        }
    }

    public init(cells: [Cell]) throws {
        let r = CellReader(cells)
        let typename = try r.req(0, \.asText)
        switch typename {
        case "ScreenStateRef": self = .stateRef(try r.obj(1, ScreenStateRef.init(cells:)))
        case "ScreenTextLiteral": self = .textLiteral(try r.obj(1, ScreenTextLiteral.init(cells:)))
        default: self = .unknown(typename: typename)
        }
    }

    public var kind: ScreenTextValueKind? {
        switch self {
        case .stateRef: return .stateRef
        case .textLiteral: return .textLiteral
        case .unknown: return nil
        }
    }
}

public enum ScreenTextValueKind: String, Codable, Sendable {
    case stateRef = "ScreenStateRef"
    case textLiteral = "ScreenTextLiteral"
}

public enum ScreenFlagValue: Codable, Sendable, Equatable {
    case flagLiteral(ScreenFlagLiteral)
    case stateRef(ScreenStateRef)
    case unknown(typename: String)

    public init(from decoder: Decoder) throws {
        let typename = try decoder.container(keyedBy: UnionTypenameKey.self).decode(String.self, forKey: .typename)
        switch typename {
        case "ScreenFlagLiteral": self = .flagLiteral(try ScreenFlagLiteral(from: decoder))
        case "ScreenStateRef": self = .stateRef(try ScreenStateRef(from: decoder))
        default: self = .unknown(typename: typename)
        }
    }

    public func encode(to encoder: Encoder) throws {
        switch self {
        case let .flagLiteral(value): try JSONValue.tagged("ScreenFlagLiteral", value).encode(to: encoder)
        case let .stateRef(value): try JSONValue.tagged("ScreenStateRef", value).encode(to: encoder)
        case let .unknown(typename): try JSONValue.tagged(typename, [String: String]()).encode(to: encoder)
        }
    }

    public init(cells: [Cell]) throws {
        let r = CellReader(cells)
        let typename = try r.req(0, \.asText)
        switch typename {
        case "ScreenFlagLiteral": self = .flagLiteral(try r.obj(1, ScreenFlagLiteral.init(cells:)))
        case "ScreenStateRef": self = .stateRef(try r.obj(1, ScreenStateRef.init(cells:)))
        default: self = .unknown(typename: typename)
        }
    }

    public var kind: ScreenFlagValueKind? {
        switch self {
        case .flagLiteral: return .flagLiteral
        case .stateRef: return .stateRef
        case .unknown: return nil
        }
    }
}

public enum ScreenFlagValueKind: String, Codable, Sendable {
    case flagLiteral = "ScreenFlagLiteral"
    case stateRef = "ScreenStateRef"
}

public struct ScreenColumn: Codable, Sendable, Equatable {
    public let id: String
    public let children: [String]
    public let press: ScreenAction?
    public let visible: ScreenVisibility?
    public let gap: ScreenNumberValue?
    public let alignItems: String?

    public init(id: String, children: [String], press: ScreenAction? = nil, visible: ScreenVisibility? = nil, gap: ScreenNumberValue? = nil, alignItems: String? = nil) {
        self.id = id
        self.children = children
        self.press = press
        self.visible = visible
        self.gap = gap
        self.alignItems = alignItems
    }

    private enum CodingKeys: String, CodingKey {
        case id = "id"
        case children = "children"
        case press = "press"
        case visible = "visible"
        case gap = "gap"
        case alignItems = "alignItems"
    }

    public init(from decoder: Decoder) throws {
        let container = try decoder.container(keyedBy: CodingKeys.self)
        id = try container.decode(IDText.self, forKey: .id).value
        children = try container.decode([IDText].self, forKey: .children).map(\.value)
        press = try container.decodeIfPresent(ScreenAction.self, forKey: .press)
        visible = try container.decodeIfPresent(ScreenVisibility.self, forKey: .visible)
        gap = try container.decodeIfPresent(ScreenNumberValue.self, forKey: .gap)
        alignItems = try container.decodeIfPresent(String.self, forKey: .alignItems)
    }

    public init(cells: [Cell]) throws {
        let r = CellReader(cells)
        id = try r.req(0, \.asId)
        children = try r.list(1, \.asId)
        press = try r.objOpt(2, ScreenAction.init(cells:))
        visible = try r.objOpt(3, ScreenVisibility.init(cells:))
        gap = try r.objOpt(4, ScreenNumberValue.init(cells:))
        alignItems = try r.opt(5, \.asText)
    }
}

public enum ScreenNumberValue: Codable, Sendable, Equatable {
    case numberLiteral(ScreenNumberLiteral)
    case stateRef(ScreenStateRef)
    case unknown(typename: String)

    public init(from decoder: Decoder) throws {
        let typename = try decoder.container(keyedBy: UnionTypenameKey.self).decode(String.self, forKey: .typename)
        switch typename {
        case "ScreenNumberLiteral": self = .numberLiteral(try ScreenNumberLiteral(from: decoder))
        case "ScreenStateRef": self = .stateRef(try ScreenStateRef(from: decoder))
        default: self = .unknown(typename: typename)
        }
    }

    public func encode(to encoder: Encoder) throws {
        switch self {
        case let .numberLiteral(value): try JSONValue.tagged("ScreenNumberLiteral", value).encode(to: encoder)
        case let .stateRef(value): try JSONValue.tagged("ScreenStateRef", value).encode(to: encoder)
        case let .unknown(typename): try JSONValue.tagged(typename, [String: String]()).encode(to: encoder)
        }
    }

    public init(cells: [Cell]) throws {
        let r = CellReader(cells)
        let typename = try r.req(0, \.asText)
        switch typename {
        case "ScreenNumberLiteral": self = .numberLiteral(try r.obj(1, ScreenNumberLiteral.init(cells:)))
        case "ScreenStateRef": self = .stateRef(try r.obj(1, ScreenStateRef.init(cells:)))
        default: self = .unknown(typename: typename)
        }
    }

    public var kind: ScreenNumberValueKind? {
        switch self {
        case .numberLiteral: return .numberLiteral
        case .stateRef: return .stateRef
        case .unknown: return nil
        }
    }
}

public enum ScreenNumberValueKind: String, Codable, Sendable {
    case numberLiteral = "ScreenNumberLiteral"
    case stateRef = "ScreenStateRef"
}

public struct ScreenHeading: Codable, Sendable, Equatable {
    public let id: String
    public let children: [String]
    public let press: ScreenAction?
    public let visible: ScreenVisibility?
    public let text: ScreenTextValue?
    public let level: Int?

    public init(id: String, children: [String], press: ScreenAction? = nil, visible: ScreenVisibility? = nil, text: ScreenTextValue? = nil, level: Int? = nil) {
        self.id = id
        self.children = children
        self.press = press
        self.visible = visible
        self.text = text
        self.level = level
    }

    private enum CodingKeys: String, CodingKey {
        case id = "id"
        case children = "children"
        case press = "press"
        case visible = "visible"
        case text = "text"
        case level = "level"
    }

    public init(from decoder: Decoder) throws {
        let container = try decoder.container(keyedBy: CodingKeys.self)
        id = try container.decode(IDText.self, forKey: .id).value
        children = try container.decode([IDText].self, forKey: .children).map(\.value)
        press = try container.decodeIfPresent(ScreenAction.self, forKey: .press)
        visible = try container.decodeIfPresent(ScreenVisibility.self, forKey: .visible)
        text = try container.decodeIfPresent(ScreenTextValue.self, forKey: .text)
        level = try container.decodeIfPresent(Int.self, forKey: .level)
    }

    public init(cells: [Cell]) throws {
        let r = CellReader(cells)
        id = try r.req(0, \.asId)
        children = try r.list(1, \.asId)
        press = try r.objOpt(2, ScreenAction.init(cells:))
        visible = try r.objOpt(3, ScreenVisibility.init(cells:))
        text = try r.objOpt(4, ScreenTextValue.init(cells:))
        level = try r.opt(5, \.asInt)
    }
}

public struct ScreenPressable: Codable, Sendable, Equatable {
    public let id: String
    public let children: [String]
    public let press: ScreenAction?
    public let visible: ScreenVisibility?

    public init(id: String, children: [String], press: ScreenAction? = nil, visible: ScreenVisibility? = nil) {
        self.id = id
        self.children = children
        self.press = press
        self.visible = visible
    }

    private enum CodingKeys: String, CodingKey {
        case id = "id"
        case children = "children"
        case press = "press"
        case visible = "visible"
    }

    public init(from decoder: Decoder) throws {
        let container = try decoder.container(keyedBy: CodingKeys.self)
        id = try container.decode(IDText.self, forKey: .id).value
        children = try container.decode([IDText].self, forKey: .children).map(\.value)
        press = try container.decodeIfPresent(ScreenAction.self, forKey: .press)
        visible = try container.decodeIfPresent(ScreenVisibility.self, forKey: .visible)
    }

    public init(cells: [Cell]) throws {
        let r = CellReader(cells)
        id = try r.req(0, \.asId)
        children = try r.list(1, \.asId)
        press = try r.objOpt(2, ScreenAction.init(cells:))
        visible = try r.objOpt(3, ScreenVisibility.init(cells:))
    }
}

public struct ScreenSpacer: Codable, Sendable, Equatable {
    public let id: String
    public let children: [String]
    public let press: ScreenAction?
    public let visible: ScreenVisibility?
    public let length: ScreenNumberValue?

    public init(id: String, children: [String], press: ScreenAction? = nil, visible: ScreenVisibility? = nil, length: ScreenNumberValue? = nil) {
        self.id = id
        self.children = children
        self.press = press
        self.visible = visible
        self.length = length
    }

    private enum CodingKeys: String, CodingKey {
        case id = "id"
        case children = "children"
        case press = "press"
        case visible = "visible"
        case length = "length"
    }

    public init(from decoder: Decoder) throws {
        let container = try decoder.container(keyedBy: CodingKeys.self)
        id = try container.decode(IDText.self, forKey: .id).value
        children = try container.decode([IDText].self, forKey: .children).map(\.value)
        press = try container.decodeIfPresent(ScreenAction.self, forKey: .press)
        visible = try container.decodeIfPresent(ScreenVisibility.self, forKey: .visible)
        length = try container.decodeIfPresent(ScreenNumberValue.self, forKey: .length)
    }

    public init(cells: [Cell]) throws {
        let r = CellReader(cells)
        id = try r.req(0, \.asId)
        children = try r.list(1, \.asId)
        press = try r.objOpt(2, ScreenAction.init(cells:))
        visible = try r.objOpt(3, ScreenVisibility.init(cells:))
        length = try r.objOpt(4, ScreenNumberValue.init(cells:))
    }
}

public struct ScreenSwitch: Codable, Sendable, Equatable {
    public let id: String
    public let children: [String]
    public let press: ScreenAction?
    public let visible: ScreenVisibility?
    public let checked: ScreenFlagValue?
    public let label: ScreenTextValue?

    public init(id: String, children: [String], press: ScreenAction? = nil, visible: ScreenVisibility? = nil, checked: ScreenFlagValue? = nil, label: ScreenTextValue? = nil) {
        self.id = id
        self.children = children
        self.press = press
        self.visible = visible
        self.checked = checked
        self.label = label
    }

    private enum CodingKeys: String, CodingKey {
        case id = "id"
        case children = "children"
        case press = "press"
        case visible = "visible"
        case checked = "checked"
        case label = "label"
    }

    public init(from decoder: Decoder) throws {
        let container = try decoder.container(keyedBy: CodingKeys.self)
        id = try container.decode(IDText.self, forKey: .id).value
        children = try container.decode([IDText].self, forKey: .children).map(\.value)
        press = try container.decodeIfPresent(ScreenAction.self, forKey: .press)
        visible = try container.decodeIfPresent(ScreenVisibility.self, forKey: .visible)
        checked = try container.decodeIfPresent(ScreenFlagValue.self, forKey: .checked)
        label = try container.decodeIfPresent(ScreenTextValue.self, forKey: .label)
    }

    public init(cells: [Cell]) throws {
        let r = CellReader(cells)
        id = try r.req(0, \.asId)
        children = try r.list(1, \.asId)
        press = try r.objOpt(2, ScreenAction.init(cells:))
        visible = try r.objOpt(3, ScreenVisibility.init(cells:))
        checked = try r.objOpt(4, ScreenFlagValue.init(cells:))
        label = try r.objOpt(5, ScreenTextValue.init(cells:))
    }
}

public struct ScreenStateEntry: Codable, Sendable, Equatable {
    public let path: String
    public let value: ScreenStateValue

    public init(path: String, value: ScreenStateValue) {
        self.path = path
        self.value = value
    }

    private enum CodingKeys: String, CodingKey {
        case path = "path"
        case value = "value"
    }

    public init(from decoder: Decoder) throws {
        let container = try decoder.container(keyedBy: CodingKeys.self)
        path = try container.decode(String.self, forKey: .path)
        value = try container.decode(ScreenStateValue.self, forKey: .value)
    }

    public init(cells: [Cell]) throws {
        let r = CellReader(cells)
        path = try r.req(0, \.asText)
        value = try r.obj(1, ScreenStateValue.init(cells:))
    }
}

public enum ScreenStateValue: Codable, Sendable, Equatable {
    case flag(ScreenFlagLiteral)
    case number(ScreenNumberLiteral)
    case text(ScreenTextLiteral)
    case unknown(typename: String)

    public init(from decoder: Decoder) throws {
        let typename = try decoder.container(keyedBy: UnionTypenameKey.self).decode(String.self, forKey: .typename)
        switch typename {
        case "ScreenFlagLiteral": self = .flag(try ScreenFlagLiteral(from: decoder))
        case "ScreenNumberLiteral": self = .number(try ScreenNumberLiteral(from: decoder))
        case "ScreenTextLiteral": self = .text(try ScreenTextLiteral(from: decoder))
        default: self = .unknown(typename: typename)
        }
    }

    public func encode(to encoder: Encoder) throws {
        switch self {
        case let .flag(value): try JSONValue.tagged("ScreenFlagLiteral", value).encode(to: encoder)
        case let .number(value): try JSONValue.tagged("ScreenNumberLiteral", value).encode(to: encoder)
        case let .text(value): try JSONValue.tagged("ScreenTextLiteral", value).encode(to: encoder)
        case let .unknown(typename): try JSONValue.tagged(typename, [String: String]()).encode(to: encoder)
        }
    }

    public init(cells: [Cell]) throws {
        let r = CellReader(cells)
        let typename = try r.req(0, \.asText)
        switch typename {
        case "ScreenFlagLiteral": self = .flag(try r.obj(1, ScreenFlagLiteral.init(cells:)))
        case "ScreenNumberLiteral": self = .number(try r.obj(1, ScreenNumberLiteral.init(cells:)))
        case "ScreenTextLiteral": self = .text(try r.obj(1, ScreenTextLiteral.init(cells:)))
        default: self = .unknown(typename: typename)
        }
    }

    public var kind: ScreenStateValueKind? {
        switch self {
        case .flag: return .flag
        case .number: return .number
        case .text: return .text
        case .unknown: return nil
        }
    }
}

public enum ScreenStateValueKind: String, Codable, Sendable {
    case flag = "ScreenFlagLiteral"
    case number = "ScreenNumberLiteral"
    case text = "ScreenTextLiteral"
}

public enum MomentWidgetItemProps: Codable, Sendable, Equatable {
    case emoji(MomentEmojiProps)
    case label(MomentLabelProps)
    case unknown(typename: String)

    public init(from decoder: Decoder) throws {
        let typename = try decoder.container(keyedBy: UnionTypenameKey.self).decode(String.self, forKey: .typename)
        switch typename {
        case "MomentEmojiProps": self = .emoji(try MomentEmojiProps(from: decoder))
        case "MomentLabelProps": self = .label(try MomentLabelProps(from: decoder))
        default: self = .unknown(typename: typename)
        }
    }

    public func encode(to encoder: Encoder) throws {
        switch self {
        case let .emoji(value): try JSONValue.tagged("MomentEmojiProps", value).encode(to: encoder)
        case let .label(value): try JSONValue.tagged("MomentLabelProps", value).encode(to: encoder)
        case let .unknown(typename): try JSONValue.tagged(typename, [String: String]()).encode(to: encoder)
        }
    }

    public init(cells: [Cell]) throws {
        let r = CellReader(cells)
        let typename = try r.req(0, \.asText)
        switch typename {
        case "MomentEmojiProps": self = .emoji(try r.obj(1, MomentEmojiProps.init(cells:)))
        case "MomentLabelProps": self = .label(try r.obj(1, MomentLabelProps.init(cells:)))
        default: self = .unknown(typename: typename)
        }
    }

    public var kind: MomentWidgetItemPropsKind? {
        switch self {
        case .emoji: return .emoji
        case .label: return .label
        case .unknown: return nil
        }
    }
}

public enum MomentWidgetItemPropsKind: String, Codable, Sendable {
    case emoji = "MomentEmojiProps"
    case label = "MomentLabelProps"
}

public struct MomentEmojiProps: Codable, Sendable, Equatable {
    public let emoji: String

    public init(emoji: String) {
        self.emoji = emoji
    }

    private enum CodingKeys: String, CodingKey {
        case emoji = "emoji"
    }

    public init(from decoder: Decoder) throws {
        let container = try decoder.container(keyedBy: CodingKeys.self)
        emoji = try container.decode(String.self, forKey: .emoji)
    }

    public init(cells: [Cell]) throws {
        let r = CellReader(cells)
        emoji = try r.req(0, \.asText)
    }
}

public struct MomentLabelProps: Codable, Sendable, Equatable {
    public let text: String
    public let size: Int?

    public init(text: String, size: Int? = nil) {
        self.text = text
        self.size = size
    }

    private enum CodingKeys: String, CodingKey {
        case text = "text"
        case size = "size"
    }

    public init(from decoder: Decoder) throws {
        let container = try decoder.container(keyedBy: CodingKeys.self)
        text = try container.decode(String.self, forKey: .text)
        size = try container.decodeIfPresent(Int.self, forKey: .size)
    }

    public init(cells: [Cell]) throws {
        let r = CellReader(cells)
        text = try r.req(0, \.asText)
        size = try r.opt(1, \.asInt)
    }
}

public enum NestedSamplePhase: Codable, Sendable, Hashable {
    case draft
    case live
    case unknown(String)

    public init(rawValue: String) {
        switch rawValue {
        case "draft": self = .draft
        case "live": self = .live
        default: self = .unknown(rawValue)
        }
    }

    public var rawValue: String {
        switch self {
        case .draft: return "draft"
        case .live: return "live"
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

public enum NestedValueState: Codable, Sendable, Hashable {
    case ready
    case done
    case unknown(String)

    public init(rawValue: String) {
        switch rawValue {
        case "ready": self = .ready
        case "done": self = .done
        default: self = .unknown(rawValue)
        }
    }

    public var rawValue: String {
        switch self {
        case .ready: return "ready"
        case .done: return "done"
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

public enum VibeKind: Codable, Sendable, Hashable {
    case like
    case love
    case laugh
    case unknown(String)

    public init(rawValue: String) {
        switch rawValue {
        case "like": self = .like
        case "love": self = .love
        case "laugh": self = .laugh
        default: self = .unknown(rawValue)
        }
    }

    public var rawValue: String {
        switch self {
        case .like: return "like"
        case .love: return "love"
        case .laugh: return "laugh"
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

public let modelsJSON = #"[{"key":"Chat","fields":[{"name":"id","kind":{"type":"scalar","kind":"id"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"title","kind":{"type":"scalar","kind":"str"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"lastMessageAt","kind":{"type":"scalar","kind":"date"},"nullable":true,"optional":false,"default":null,"indexed":true,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"lastMessageId","kind":{"type":"scalar","kind":"id"},"nullable":true,"optional":false,"default":null,"indexed":true,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"shareUrl","kind":{"type":"scalar","kind":"str"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"isPublic","kind":{"type":"scalar","kind":"bool"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null}],"associations":[{"name":"messages","kind":"has_many","target":"Message","foreign_key":"chatId","dependent":"destroy","touches_parent":false,"sets":{},"newer":[],"from":null},{"name":"lastMessage","kind":"belongs_to","target":"Message","foreign_key":"lastMessageId","dependent":null,"touches_parent":false,"sets":{},"newer":[],"from":null}],"relations":[{"name":"node","by_columns":[],"text_params":["nodeId"],"remote":{"operation":"ChatNode","shape":{"type":"single"},"stale_time":"5.minutes","empty_stale_time":null,"selects_page_info":false,"updates":[],"page_size":null,"variables":{"id":{"from":"param","name":"nodeId"}},"root":null},"is_global":false,"sort":"server_order","joins_events":false,"joins_created":false,"node":"one"},{"name":"nodes","by_columns":[],"text_params":[],"remote":{"operation":"ChatsByIds","shape":{"type":"list","coverage":"complete"},"stale_time":"5.minutes","empty_stale_time":null,"selects_page_info":false,"updates":[],"page_size":null,"variables":{},"root":null},"is_global":true,"sort":"server_order","joins_events":false,"joins_created":false,"node":"many"},{"name":"recent","by_columns":[],"text_params":[],"remote":null,"is_global":true,"sort":{"fields":[{"column":"lastMessageAt","direction":"desc"}]},"joins_events":false,"joins_created":false}],"indexes":[],"actions":[{"name":"create","root_shape":"upsert","on_failure":"rollback","has_optimistic":true,"has_temp_row_retention":true,"has_cascade_dependents":false,"has_once":false,"dedupe_key":null,"members":[],"mutation":"CreateChat","variables":{},"optimistic":{},"refetch":[],"args":{},"local":[],"correlate":[],"ignore_response":false,"wrap":null,"root":null,"lands":[],"result":{}}],"ephemeral":[],"storage":"disk","expire_after":null,"listens":[]},{"key":"Message","fields":[{"name":"id","kind":{"type":"scalar","kind":"id"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"chatId","kind":{"type":"scalar","kind":"id"},"nullable":false,"optional":false,"default":null,"indexed":true,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"authorId","kind":{"type":"scalar","kind":"id"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"text","kind":{"type":"scalar","kind":"str"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"createdAt","kind":{"type":"scalar","kind":"date"},"nullable":false,"optional":false,"default":null,"indexed":true,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"sequenceNumber","kind":{"type":"scalar","kind":"int"},"nullable":true,"optional":false,"default":null,"indexed":true,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"attachment","kind":{"type":"union"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"MessageAttachment","merge":null,"fields":[{"name":"gift","kind":{"type":"object"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"GiftAttachment","merge":null,"fields":[{"name":"id","kind":{"type":"scalar","kind":"id"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"tier","kind":{"type":"scalar","kind":"str"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":"giftTier","app_type":null,"merge":null},{"name":"status","kind":{"type":"scalar","kind":"str"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"tags","kind":{"type":"list","element":"str"},"nullable":false,"optional":false,"default":[],"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null}]},{"name":"moment","kind":{"type":"object"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"MomentAttachment","merge":null,"fields":[{"name":"momentId","kind":{"type":"scalar","kind":"id"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"caption","kind":{"type":"scalar","kind":"str"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null}]},{"name":"profile","kind":{"type":"object"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"ProfileAttachment","merge":null,"fields":[{"name":"user","kind":{"type":"object"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"AttachmentUser","merge":null,"fields":[{"name":"id","kind":{"type":"scalar","kind":"id"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"name","kind":{"type":"scalar","kind":"str"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null}]}]},{"name":"reaction","kind":{"type":"object"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"ReactionAttachment","merge":null,"fields":[{"name":"body","kind":{"type":"scalar","kind":"str"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null}]}]},{"name":"media","kind":{"type":"object"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"MessageMedia","merge":{"type":"keys","keys":{"fileUrl":{"type":"non_empty"}}},"fields":[{"name":"kind","kind":{"type":"scalar","kind":"str"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"transcoded","kind":{"type":"scalar","kind":"bool"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"fileUrl","kind":{"type":"scalar","kind":"str"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null}]}],"associations":[{"name":"chat","kind":"belongs_to","target":"Chat","foreign_key":"chatId","dependent":null,"touches_parent":false,"sets":{"lastMessageAt":"createdAt","lastMessageId":"id"},"newer":[{"child":"createdAt","parent":"lastMessageAt"}],"from":null}],"relations":[{"name":"by_chat","by_columns":["chatId"],"text_params":[],"remote":null,"is_global":false,"sort":{"fields":[{"column":"createdAt","direction":"asc"}]},"joins_events":false,"joins_created":false},{"name":"thread","by_columns":["chatId"],"text_params":[],"remote":{"operation":"ChatThread","shape":{"type":"connection","direction":"forward","coverage":"page","max_pages":null,"cursor":null},"stale_time":"5.minutes","empty_stale_time":null,"selects_page_info":true,"updates":[],"page_size":30,"variables":{},"root":"chat.messages"},"is_global":false,"sort":{"fields":[{"column":"sequenceNumber","direction":"asc_nulls_last"},{"column":"createdAt","direction":"asc"}]},"joins_events":true,"joins_created":false},{"name":"transcode_status","by_columns":[],"text_params":["nodeId"],"remote":{"operation":"MessageTranscodeStatus","shape":{"type":"single"},"stale_time":"5.minutes","empty_stale_time":null,"selects_page_info":false,"updates":[],"page_size":null,"variables":{"id":{"from":"param","name":"nodeId"}},"root":null},"is_global":false,"sort":"server_order","joins_events":false,"joins_created":false,"node":"one"},{"name":"chat_media","by_columns":["chatId"],"text_params":[],"remote":{"operation":"ChatMedia","shape":{"type":"connection","direction":"forward","coverage":"page","max_pages":null,"cursor":null},"stale_time":"5.minutes","empty_stale_time":null,"selects_page_info":true,"updates":[],"page_size":30,"variables":{},"root":"node.messages"},"is_global":false,"sort":"server_order","joins_events":false,"joins_created":false},{"name":"history","by_columns":[],"text_params":[],"remote":{"operation":"ChatMessages","shape":{"type":"connection","direction":"backward","coverage":"page","max_pages":null,"cursor":null},"stale_time":"1.minute","empty_stale_time":"10.seconds","selects_page_info":true,"updates":[],"page_size":30,"variables":{},"root":null},"is_global":true,"sort":"server_order","joins_events":false,"joins_created":false}],"indexes":[],"actions":[{"name":"create","root_shape":"upsert","on_failure":"rollback","has_optimistic":true,"has_temp_row_retention":true,"has_cascade_dependents":false,"has_once":false,"dedupe_key":null,"members":[],"mutation":"CreateMessage","variables":{"attachedUserId":"attachedUserId","chatId":"chatId","signedId":"signedId","text":"text"},"optimistic":{},"refetch":[],"args":{"attachedUserId":"id?","signedId":"string?"},"local":["authorId"],"correlate":[],"ignore_response":false,"wrap":null,"root":null,"lands":[],"result":{}}],"ephemeral":[],"storage":"disk","expire_after":null,"listens":[]},{"key":"Moment","fields":[{"name":"id","kind":{"type":"scalar","kind":"id"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"uuid","kind":{"type":"scalar","kind":"str"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"userId","kind":{"type":"scalar","kind":"id"},"nullable":true,"optional":false,"default":null,"indexed":true,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"text","kind":{"type":"scalar","kind":"str"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null,"searchable":true},{"name":"likedByMe","kind":{"type":"scalar","kind":"bool"},"nullable":false,"optional":false,"default":false,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"media","kind":{"type":"object"},"nullable":false,"optional":false,"default":{},"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"tone","kind":{"type":"scalar","kind":"str"},"nullable":false,"optional":true,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":{"type":"local"}},{"name":"createdAt","kind":{"type":"scalar","kind":"date"},"nullable":false,"optional":false,"default":null,"indexed":true,"id_from_parent":null,"from":null,"app_type":null,"merge":null}],"associations":[{"name":"author","kind":"belongs_to","target":"User","foreign_key":"userId","dependent":null,"touches_parent":true,"sets":{},"newer":[],"from":null},{"name":"vibes","kind":"has_many","target":"Vibe","foreign_key":"momentId","dependent":"destroy","touches_parent":false,"sets":{},"newer":[],"from":null}],"relations":[{"name":"transcode_status","by_columns":[],"text_params":["nodeId"],"remote":{"operation":"MomentTranscodeStatus","shape":{"type":"single"},"stale_time":"5.minutes","empty_stale_time":null,"selects_page_info":false,"updates":[],"page_size":null,"variables":{"momentId":{"from":"param","name":"nodeId"}},"root":null},"is_global":false,"sort":"server_order","joins_events":false,"joins_created":false,"node":"one"},{"name":"recent","by_columns":[],"text_params":[],"remote":null,"is_global":true,"sort":{"fields":[{"column":"createdAt","direction":"desc"}]},"joins_events":false,"joins_created":false},{"name":"by_user","by_columns":["userId"],"text_params":[],"remote":null,"is_global":false,"sort":{"fields":[{"column":"createdAt","direction":"desc"}]},"joins_events":false,"joins_created":false},{"name":"feed","by_columns":[],"text_params":[],"remote":{"operation":"MomentsFeed","shape":{"type":"connection","direction":"forward","coverage":"page","max_pages":null,"cursor":null},"stale_time":"5.minutes","empty_stale_time":null,"selects_page_info":true,"updates":[{"subscription":"MomentEvents","deleted_by":"destroyed","set":{},"removes":false,"debounce":null,"root":null,"id":null}],"page_size":20,"variables":{},"root":null},"is_global":true,"sort":"server_order","joins_events":false,"joins_created":false}],"indexes":[["uuid"],["userId","createdAt"]],"actions":[{"name":"create","root_shape":"upsert","on_failure":"rollback","has_optimistic":true,"has_temp_row_retention":true,"has_cascade_dependents":false,"has_once":false,"dedupe_key":null,"members":[],"mutation":"CreateMoment","variables":{"media":"media","text":"text"},"optimistic":{},"refetch":[],"args":{},"local":[],"correlate":[],"ignore_response":false,"wrap":null,"root":null,"lands":[],"result":{}},{"name":"update","root_shape":"upsert","on_failure":"rollback","has_optimistic":false,"has_temp_row_retention":false,"has_cascade_dependents":false,"has_once":false,"dedupe_key":null,"members":[],"mutation":"UpdateMoment","variables":{},"optimistic":{},"refetch":[],"args":{},"local":[],"correlate":[],"ignore_response":false,"wrap":null,"root":null,"lands":[],"result":{}},{"name":"destroy","root_shape":"destroy","on_failure":"rollback","has_optimistic":false,"has_temp_row_retention":false,"has_cascade_dependents":false,"has_once":false,"dedupe_key":null,"members":[],"mutation":"DeleteMoment","variables":{},"optimistic":{},"refetch":[],"args":{},"local":[],"correlate":[],"ignore_response":false,"wrap":null,"root":null,"lands":[],"result":{}},{"name":"like","root_shape":"upsert","on_failure":"rollback","has_optimistic":false,"has_temp_row_retention":false,"has_cascade_dependents":false,"has_once":false,"dedupe_key":null,"members":[],"mutation":"LikeMoment","variables":{"momentId":"id"},"optimistic":{"likedByMe":true},"refetch":["feed"],"args":{},"local":[],"correlate":[],"ignore_response":false,"wrap":null,"root":null,"lands":[],"result":{}},{"name":"share","root_shape":"upsert","on_failure":"rollback","has_optimistic":false,"has_temp_row_retention":false,"has_cascade_dependents":false,"has_once":false,"dedupe_key":null,"members":[],"mutation":"ShareMoment","variables":{"momentId":"id"},"optimistic":{},"refetch":[],"args":{},"local":[],"correlate":[],"ignore_response":false,"wrap":null,"root":"shareMoment.moment","lands":[],"result":{"copies":"int","successMessage":"string"}}],"ephemeral":[],"storage":"disk","expire_after":null,"listens":[]},{"key":"NestedSample","fields":[{"name":"id","kind":{"type":"scalar","kind":"id"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"phase","kind":{"type":"enum","values":["draft","live"]},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"NestedSamplePhase","merge":null},{"name":"value","kind":{"type":"object"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"NestedValue","merge":null,"fields":[{"name":"title","kind":{"type":"scalar","kind":"str"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"note","kind":{"type":"scalar","kind":"str"},"nullable":false,"optional":true,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"nullableDefault","kind":{"type":"scalar","kind":"str"},"nullable":false,"optional":true,"default":"default","indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"ownerId","kind":{"type":"scalar","kind":"id"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":"owner_id","app_type":null,"merge":null},{"name":"count","kind":{"type":"scalar","kind":"int"},"nullable":false,"optional":false,"default":2,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"ratio","kind":{"type":"scalar","kind":"num"},"nullable":false,"optional":false,"default":1.5,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"enabled","kind":{"type":"scalar","kind":"bool"},"nullable":false,"optional":false,"default":false,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"createdAt","kind":{"type":"scalar","kind":"date"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"state","kind":{"type":"enum","values":["ready","done"]},"nullable":false,"optional":false,"default":"ready","indexed":false,"id_from_parent":null,"from":null,"app_type":"NestedValueState","merge":null},{"name":"labels","kind":{"type":"list","element":"str"},"nullable":false,"optional":false,"default":[],"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"counts","kind":{"type":"list","element":"int"},"nullable":false,"optional":false,"default":[],"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"ratios","kind":{"type":"list","element":"num"},"nullable":false,"optional":false,"default":[],"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"flags","kind":{"type":"list","element":"bool"},"nullable":false,"optional":false,"default":[],"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"dates","kind":{"type":"list","element":"date"},"nullable":false,"optional":false,"default":[],"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"owners","kind":{"type":"list","element":"id"},"nullable":false,"optional":false,"default":[],"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"payload","kind":{"type":"raw"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"rawValue","kind":{"type":"raw"},"nullable":false,"optional":false,"default":{"free":[1,true,null]},"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"preferences","kind":{"type":"object"},"nullable":false,"optional":false,"default":{"label":"base"},"indexed":false,"id_from_parent":null,"from":null,"app_type":"NestedValuePreferences","merge":null,"fields":[{"name":"label","kind":{"type":"scalar","kind":"str"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"active","kind":{"type":"scalar","kind":"bool"},"nullable":false,"optional":false,"default":true,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null}]},{"name":"presets","kind":{"type":"array"},"nullable":false,"optional":false,"default":[{"label":"first"}],"indexed":false,"id_from_parent":null,"from":null,"app_type":"NestedValuePreferences","merge":null},{"name":"metadata","kind":{"type":"object"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"values","kind":{"type":"array"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"child","kind":{"type":"object"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"NestedValueChild","merge":null,"fields":[{"name":"body","kind":{"type":"scalar","kind":"str"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"horizontal","kind":{"type":"scalar","kind":"num"},"nullable":false,"optional":false,"default":0,"indexed":false,"id_from_parent":null,"from":"x","app_type":null,"merge":null}]},{"name":"children","kind":{"type":"array"},"nullable":false,"optional":false,"default":[],"indexed":false,"id_from_parent":null,"from":null,"app_type":"NestedValueChild","merge":null}]},{"name":"values","kind":{"type":"array"},"nullable":false,"optional":false,"default":[],"indexed":false,"id_from_parent":null,"from":null,"app_type":"NestedValue","merge":null},{"name":"tree","kind":{"type":"object"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"NestedTree","merge":null,"fields":[{"name":"name","kind":{"type":"scalar","kind":"str"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"children","kind":{"type":"array"},"nullable":false,"optional":false,"default":[],"indexed":false,"id_from_parent":null,"from":null,"app_type":"NestedTree","merge":null}]},{"name":"replyTo","kind":{"type":"object"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"MessageReplyNode","merge":null,"fields":[{"name":"id","kind":{"type":"scalar","kind":"id"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"body","kind":{"type":"scalar","kind":"str"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"kind","kind":{"type":"scalar","kind":"str"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"userId","kind":{"type":"scalar","kind":"id"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"user","kind":{"type":"object"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"MessageUserRef","merge":null,"fields":[{"name":"id","kind":{"type":"scalar","kind":"id"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"fullName","kind":{"type":"scalar","kind":"str"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"name","kind":{"type":"scalar","kind":"str"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"username","kind":{"type":"scalar","kind":"str"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"avatarUrl","kind":{"type":"scalar","kind":"str"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null}]},{"name":"media","kind":{"type":"object"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"NestedMedia","merge":null,"fields":[{"name":"id","kind":{"type":"scalar","kind":"id"},"nullable":false,"optional":false,"default":"","indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"fileUrl","kind":{"type":"scalar","kind":"str"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null}]}]}],"associations":[],"relations":[],"indexes":[],"actions":[],"ephemeral":[],"storage":"local","expire_after":null,"listens":[]},{"key":"NestedReference","fields":[{"name":"id","kind":{"type":"scalar","kind":"id"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"values","kind":{"type":"array"},"nullable":false,"optional":false,"default":[],"indexed":false,"id_from_parent":null,"from":null,"app_type":"NestedValue","merge":null}],"associations":[],"relations":[],"indexes":[],"actions":[],"ephemeral":[],"storage":"local","expire_after":null,"listens":[]},{"key":"Presence","fields":[{"name":"id","kind":{"type":"scalar","kind":"id"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"userId","kind":{"type":"scalar","kind":"id"},"nullable":false,"optional":false,"default":null,"indexed":true,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"room","kind":{"type":"scalar","kind":"str"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null}],"associations":[{"name":"user","kind":"belongs_to","target":"User","foreign_key":"userId","dependent":null,"touches_parent":false,"sets":{},"newer":[],"from":null}],"relations":[{"name":"for_user","by_columns":["userId"],"text_params":[],"remote":null,"is_global":false,"sort":"server_order","joins_events":false,"joins_created":false},{"name":"online","by_columns":[],"text_params":[],"remote":{"operation":"PresenceOnline","shape":{"type":"list","coverage":"complete"},"stale_time":"30.seconds","empty_stale_time":null,"selects_page_info":false,"updates":[{"subscription":"PresenceChanged","deleted_by":"left","set":{},"removes":false,"debounce":null,"root":null,"id":null}],"page_size":null,"variables":{},"root":null},"is_global":true,"sort":"server_order","joins_events":false,"joins_created":false}],"indexes":[],"actions":[],"ephemeral":[],"storage":"memory","expire_after":"1.minute","listens":[]},{"key":"CustomScreen","fields":[{"name":"id","kind":{"type":"scalar","kind":"id"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"slug","kind":{"type":"scalar","kind":"str"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"spec","kind":{"type":"object"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"ScreenSpec","merge":null,"fields":[{"name":"root","kind":{"type":"scalar","kind":"id"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"elements","kind":{"type":"union_array"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"ScreenElement","merge":null,"fields":[{"name":"button","kind":{"type":"object"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"ScreenButton","merge":null,"fields":[{"name":"label","kind":{"type":"union"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"ScreenTextValue","merge":null,"fields":[{"name":"stateRef","kind":{"type":"object"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"ScreenStateRef","merge":null},{"name":"textLiteral","kind":{"type":"object"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"ScreenTextLiteral","merge":null}]},{"name":"disabled","kind":{"type":"union"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"ScreenFlagValue","merge":null,"fields":[{"name":"flagLiteral","kind":{"type":"object"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"ScreenFlagLiteral","merge":null},{"name":"stateRef","kind":{"type":"object"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"ScreenStateRef","merge":null}]}]},{"name":"column","kind":{"type":"object"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"ScreenColumn","merge":null,"fields":[{"name":"gap","kind":{"type":"union"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"ScreenNumberValue","merge":null,"fields":[{"name":"numberLiteral","kind":{"type":"object"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"ScreenNumberLiteral","merge":null},{"name":"stateRef","kind":{"type":"object"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"ScreenStateRef","merge":null}]},{"name":"alignItems","kind":{"type":"scalar","kind":"str"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null}]},{"name":"heading","kind":{"type":"object"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"ScreenHeading","merge":null,"fields":[{"name":"text","kind":{"type":"union"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"ScreenTextValue","merge":null},{"name":"level","kind":{"type":"scalar","kind":"int"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null}]},{"name":"pressable","kind":{"type":"object"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"ScreenPressable","merge":null},{"name":"spacer","kind":{"type":"object"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"ScreenSpacer","merge":null,"fields":[{"name":"length","kind":{"type":"union"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"ScreenNumberValue","merge":null}]},{"name":"switch","kind":{"type":"object"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"ScreenSwitch","merge":null,"fields":[{"name":"checked","kind":{"type":"union"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"ScreenFlagValue","merge":null},{"name":"label","kind":{"type":"union"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"ScreenTextValue","merge":null}]}],"shared":[{"name":"id","kind":{"type":"scalar","kind":"id"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"children","kind":{"type":"list","element":"id"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"press","kind":{"type":"union"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"ScreenAction","merge":null,"fields":[{"name":"navigate","kind":{"type":"object"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"ScreenNavigateAction","merge":null,"fields":[{"name":"slug","kind":{"type":"scalar","kind":"str"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"title","kind":{"type":"scalar","kind":"str"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null}]},{"name":"openChat","kind":{"type":"object"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"ScreenOpenChatAction","merge":null,"fields":[{"name":"chatId","kind":{"type":"scalar","kind":"str"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null}]},{"name":"openExternal","kind":{"type":"object"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"ScreenOpenExternalAction","merge":null,"fields":[{"name":"url","kind":{"type":"scalar","kind":"str"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null}]},{"name":"openProfile","kind":{"type":"object"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"ScreenOpenProfileAction","merge":null,"fields":[{"name":"userId","kind":{"type":"scalar","kind":"str"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null}]}]},{"name":"visible","kind":{"type":"union"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"ScreenVisibility","merge":null,"fields":[{"name":"condition","kind":{"type":"object"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"ScreenCondition","merge":null,"fields":[{"name":"comparison","kind":{"type":"scalar","kind":"str"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"negated","kind":{"type":"scalar","kind":"bool"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"operand","kind":{"type":"union"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"ScreenOperand","merge":null,"fields":[{"name":"flagLiteral","kind":{"type":"object"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"ScreenFlagLiteral","merge":null,"fields":[{"name":"flag","kind":{"type":"scalar","kind":"bool"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null}]},{"name":"numberLiteral","kind":{"type":"object"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"ScreenNumberLiteral","merge":null,"fields":[{"name":"number","kind":{"type":"scalar","kind":"num"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null}]},{"name":"stateRef","kind":{"type":"object"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"ScreenStateRef","merge":null,"fields":[{"name":"path","kind":{"type":"scalar","kind":"str"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null}]},{"name":"textLiteral","kind":{"type":"object"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"ScreenTextLiteral","merge":null,"fields":[{"name":"text","kind":{"type":"scalar","kind":"str"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null}]}]},{"name":"path","kind":{"type":"scalar","kind":"str"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null}]},{"name":"visibleAll","kind":{"type":"object"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"ScreenVisibleAll","merge":null,"fields":[{"name":"conditions","kind":{"type":"array"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"ScreenCondition","merge":null}]},{"name":"visibleAny","kind":{"type":"object"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"ScreenVisibleAny","merge":null,"fields":[{"name":"conditions","kind":{"type":"array"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"ScreenCondition","merge":null}]},{"name":"visibleFlag","kind":{"type":"object"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"ScreenVisibleFlag","merge":null,"fields":[{"name":"flag","kind":{"type":"scalar","kind":"bool"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null}]}]}]},{"name":"state","kind":{"type":"array"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"ScreenStateEntry","merge":null,"fields":[{"name":"path","kind":{"type":"scalar","kind":"str"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"value","kind":{"type":"union"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"ScreenStateValue","merge":null,"fields":[{"name":"flag","kind":{"type":"object"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"ScreenFlagLiteral","merge":null},{"name":"number","kind":{"type":"object"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"ScreenNumberLiteral","merge":null},{"name":"text","kind":{"type":"object"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"ScreenTextLiteral","merge":null}]}]}]}],"associations":[],"relations":[{"name":"node","by_columns":[],"text_params":["nodeId"],"remote":{"operation":"ScreenNode","shape":{"type":"single"},"stale_time":"5.minutes","empty_stale_time":null,"selects_page_info":false,"updates":[],"page_size":null,"variables":{"id":{"from":"param","name":"nodeId"}},"root":null},"is_global":false,"sort":"server_order","joins_events":false,"joins_created":false,"node":"one"}],"indexes":[],"actions":[],"ephemeral":[],"storage":"disk","expire_after":null,"listens":[]},{"key":"StorageRow","fields":[{"name":"id","kind":{"type":"scalar","kind":"id"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"record","kind":{"type":"scalar","kind":"str"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null}],"associations":[],"relations":[],"indexes":[],"actions":[],"ephemeral":[],"storage":"disk","expire_after":null,"listens":[]},{"key":"UnionSample","fields":[{"name":"id","kind":{"type":"scalar","kind":"id"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"label","kind":{"type":"scalar","kind":"str"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"attachment","kind":{"type":"union"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"MessageAttachment","merge":null},{"name":"props","kind":{"type":"union"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"MomentWidgetItemProps","merge":null,"fields":[{"name":"emoji","kind":{"type":"object"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"MomentEmojiProps","merge":null,"fields":[{"name":"emoji","kind":{"type":"scalar","kind":"str"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null}]},{"name":"label","kind":{"type":"object"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"MomentLabelProps","merge":null,"fields":[{"name":"text","kind":{"type":"scalar","kind":"str"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"size","kind":{"type":"scalar","kind":"int"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null}]}]}],"associations":[],"relations":[],"indexes":[],"actions":[],"ephemeral":[],"storage":"local","expire_after":null,"listens":[]},{"key":"User","fields":[{"name":"id","kind":{"type":"scalar","kind":"id"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"name","kind":{"type":"scalar","kind":"str"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"avatar","kind":{"type":"file"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"birthday","kind":{"type":"scalar","kind":"date"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null}],"associations":[],"relations":[{"name":"profile","by_columns":[],"text_params":[],"remote":{"operation":"UserProfile","shape":{"type":"single"},"stale_time":"5.minutes","empty_stale_time":null,"selects_page_info":false,"updates":[{"subscription":"UserTyping","deleted_by":null,"set":{"typing":true},"removes":false,"debounce":null,"root":null,"id":null}],"page_size":null,"variables":{},"root":null},"is_global":true,"sort":"server_order","joins_events":false,"joins_created":false}],"indexes":[],"actions":[],"ephemeral":[{"name":"typing","kind":{"type":"scalar","kind":"bool"},"nullable":false,"optional":false,"default":false,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null,"reset_after":"5.seconds"},{"name":"mood","kind":{"type":"scalar","kind":"str"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null,"reset_after":null}],"storage":"disk","expire_after":null,"listens":[]},{"key":"Vibe","fields":[{"name":"id","kind":{"type":"scalar","kind":"id"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"momentId","kind":{"type":"scalar","kind":"id"},"nullable":false,"optional":false,"default":null,"indexed":true,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"userId","kind":{"type":"scalar","kind":"id"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"kind","kind":{"type":"enum","values":["like","love","laugh"]},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"VibeKind","merge":null}],"associations":[],"relations":[],"indexes":[["userId"]],"actions":[],"ephemeral":[],"storage":"disk","expire_after":null,"listens":[]}]"#
