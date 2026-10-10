import Foundation
import DBlayer

public struct Chat: DBModel {
    public static let modelName = "Chat"
    public let id: String
    public let title: String
    public let lastMessageAt: String?
    public let sync: SyncInfo
    public let upload: UploadInfo?

    public init(id: String, title: String, lastMessageAt: String? = nil, sync: SyncInfo = SyncInfo(), upload: UploadInfo? = nil) {
        self.id = id
        self.title = title
        self.lastMessageAt = lastMessageAt
        self.sync = sync
        self.upload = upload
    }

    public static let title = Column<Chat, String>("title")
    public static let lastMessageAt = Column<Chat, String?>("lastMessageAt")
    public static func find(_ id: String) -> LiveSource<Self?> { Query<Self>().find(id) }
    public static func `where`(_ condition: Condition) -> Query<Self> { Query<Self>().where(condition) }
    public static var recent: Query<Self> {
        var built = Query<Self>(relation: "recent", params: [:])
        built = built.order(Order(field: "lastMessageAt", direction: "desc"))
        return built
    }
    public static func create(title: String, lastMessageAt: String? = nil) async throws -> Self {
        var values: [String: JSONValue] = [:]
        values["title"] = JSONValue.encode(title)
        if let value = lastMessageAt { values["lastMessageAt"] = JSONValue.encode(value) }
        return try await create(values: values)
    }
    public func update(title: String? = nil, lastMessageAt: String?? = nil) async throws -> Self {
        var values: [String: JSONValue] = [:]
        if let value = title { values["title"] = JSONValue.encode(value) }
        if let value = lastMessageAt { values["lastMessageAt"] = JSONValue.encode(value) }
        return try await update(values: values)
    }
    public var messages: LiveSource<[Message]> { related("messages", as: Message.self) }
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
    public static func create(chatId: String, authorId: String, text: String, createdAt: String) async throws -> Self {
        var values: [String: JSONValue] = [:]
        values["chatId"] = JSONValue.encode(chatId)
        values["authorId"] = JSONValue.encode(authorId)
        values["text"] = JSONValue.encode(text)
        values["createdAt"] = JSONValue.encode(createdAt)
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
    public let createdAt: String
    public let sync: SyncInfo
    public let upload: UploadInfo?

    public init(id: String, uuid: String, userId: String? = nil, text: String? = nil, likedByMe: Bool = false, media: JSONValue, createdAt: String, sync: SyncInfo = SyncInfo(), upload: UploadInfo? = nil) {
        self.id = id
        self.uuid = uuid
        self.userId = userId
        self.text = text
        self.likedByMe = likedByMe
        self.media = media
        self.createdAt = createdAt
        self.sync = sync
        self.upload = upload
    }

    public static let uuid = Column<Moment, String>("uuid")
    public static let userId = Column<Moment, String?>("userId")
    public static let text = Column<Moment, String?>("text")
    public static let likedByMe = Column<Moment, Bool>("likedByMe")
    public static let media = Column<Moment, JSONValue>("media")
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

public struct User: DBModel {
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

public let modelsJSON = #"[{"key":"Chat","fields":[{"name":"id","kind":{"type":"scalar","kind":"id"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null},{"name":"title","kind":{"type":"scalar","kind":"str"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null},{"name":"lastMessageAt","kind":{"type":"scalar","kind":"date"},"nullable":true,"optional":false,"default":null,"indexed":true,"id_from_parent":null,"from":null,"app_type":null}],"associations":[{"name":"messages","kind":"has_many","target":"Message","foreign_key":"chatId","dependent":"destroy","touches_parent":false,"from":null}],"relations":[{"name":"recent","by_columns":[],"text_params":[],"remote":null,"is_global":true,"sort":{"fields":[{"column":"lastMessageAt","direction":"desc"}]}}],"composite_indexes":[],"actions":[],"ephemeral":[],"storage":"disk","expire_after":null,"listens":[]},{"key":"Message","fields":[{"name":"id","kind":{"type":"scalar","kind":"id"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null},{"name":"chatId","kind":{"type":"scalar","kind":"id"},"nullable":false,"optional":false,"default":null,"indexed":true,"id_from_parent":null,"from":null,"app_type":null},{"name":"authorId","kind":{"type":"scalar","kind":"id"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null},{"name":"text","kind":{"type":"scalar","kind":"str"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null},{"name":"createdAt","kind":{"type":"scalar","kind":"date"},"nullable":false,"optional":false,"default":null,"indexed":true,"id_from_parent":null,"from":null,"app_type":null}],"associations":[{"name":"chat","kind":"belongs_to","target":"Chat","foreign_key":"chatId","dependent":null,"touches_parent":false,"from":null}],"relations":[{"name":"by_chat","by_columns":["chatId"],"text_params":[],"remote":null,"is_global":false,"sort":{"fields":[{"column":"createdAt","direction":"asc"}]}},{"name":"history","by_columns":[],"text_params":[],"remote":{"operation":"ChatMessages","shape":{"type":"connection","direction":"backward","coverage":"page","max_pages":null,"cursor":null},"stale_time":"1.minute","empty_stale_time":"10.seconds","selects_page_info":true,"updates":[],"page_size":30,"variables":{},"root":null},"is_global":true,"sort":"server_order"}],"composite_indexes":[],"actions":[],"ephemeral":[],"storage":"disk","expire_after":null,"listens":[]},{"key":"Moment","fields":[{"name":"id","kind":{"type":"scalar","kind":"id"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null},{"name":"uuid","kind":{"type":"scalar","kind":"str"},"nullable":false,"optional":false,"default":null,"indexed":true,"id_from_parent":null,"from":null,"app_type":null},{"name":"userId","kind":{"type":"scalar","kind":"id"},"nullable":true,"optional":false,"default":null,"indexed":true,"id_from_parent":null,"from":null,"app_type":null},{"name":"text","kind":{"type":"scalar","kind":"str"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null},{"name":"likedByMe","kind":{"type":"scalar","kind":"bool"},"nullable":false,"optional":false,"default":false,"indexed":false,"id_from_parent":null,"from":null,"app_type":null},{"name":"media","kind":{"type":"object"},"nullable":false,"optional":false,"default":{},"indexed":false,"id_from_parent":null,"from":null,"app_type":null},{"name":"createdAt","kind":{"type":"scalar","kind":"date"},"nullable":false,"optional":false,"default":null,"indexed":true,"id_from_parent":null,"from":null,"app_type":null}],"associations":[{"name":"author","kind":"belongs_to","target":"User","foreign_key":"userId","dependent":null,"touches_parent":true,"from":null},{"name":"vibes","kind":"has_many","target":"Vibe","foreign_key":"momentId","dependent":"destroy","touches_parent":false,"from":null}],"relations":[{"name":"recent","by_columns":[],"text_params":[],"remote":null,"is_global":true,"sort":{"fields":[{"column":"createdAt","direction":"desc"}]}},{"name":"by_user","by_columns":["userId"],"text_params":[],"remote":null,"is_global":false,"sort":{"fields":[{"column":"createdAt","direction":"desc"}]}},{"name":"feed","by_columns":[],"text_params":[],"remote":{"operation":"MomentsFeed","shape":{"type":"connection","direction":"forward","coverage":"page","max_pages":null,"cursor":null},"stale_time":"5.minutes","empty_stale_time":null,"selects_page_info":true,"updates":[{"subscription":"MomentEvents","deleted_by":"destroyed","set":{},"removes":false,"debounce":null,"root":null,"id":null}],"page_size":20,"variables":{},"root":null},"is_global":true,"sort":"server_order"}],"composite_indexes":[["userId","createdAt"]],"actions":[{"name":"create","root_shape":"upsert","on_failure":"rollback","has_optimistic":true,"has_temp_row_retention":true,"has_cascade_dependents":false,"has_once":false,"dedupe_key":null,"members":[],"mutation":"CreateMoment","variables":{"media":"media","text":"text"},"optimistic":{},"refetch":[],"args":{},"local":[],"ignore_response":false,"wrap":null,"root":null,"lands":[]},{"name":"update","root_shape":"upsert","on_failure":"rollback","has_optimistic":false,"has_temp_row_retention":false,"has_cascade_dependents":false,"has_once":false,"dedupe_key":null,"members":[],"mutation":"UpdateMoment","variables":{},"optimistic":{},"refetch":[],"args":{},"local":[],"ignore_response":false,"wrap":null,"root":null,"lands":[]},{"name":"destroy","root_shape":"destroy","on_failure":"rollback","has_optimistic":false,"has_temp_row_retention":false,"has_cascade_dependents":false,"has_once":false,"dedupe_key":null,"members":[],"mutation":"DeleteMoment","variables":{},"optimistic":{},"refetch":[],"args":{},"local":[],"ignore_response":false,"wrap":null,"root":null,"lands":[]},{"name":"like","root_shape":"upsert","on_failure":"rollback","has_optimistic":false,"has_temp_row_retention":false,"has_cascade_dependents":false,"has_once":false,"dedupe_key":null,"members":[],"mutation":"LikeMoment","variables":{"momentId":"id"},"optimistic":{"likedByMe":true},"refetch":["feed"],"args":{},"local":[],"ignore_response":false,"wrap":null,"root":null,"lands":[]}],"ephemeral":[],"storage":"disk","expire_after":null,"listens":[]},{"key":"Presence","fields":[{"name":"id","kind":{"type":"scalar","kind":"id"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null},{"name":"userId","kind":{"type":"scalar","kind":"id"},"nullable":false,"optional":false,"default":null,"indexed":true,"id_from_parent":null,"from":null,"app_type":null},{"name":"room","kind":{"type":"scalar","kind":"str"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null}],"associations":[{"name":"user","kind":"belongs_to","target":"User","foreign_key":"userId","dependent":null,"touches_parent":false,"from":null}],"relations":[{"name":"for_user","by_columns":["userId"],"text_params":[],"remote":null,"is_global":false,"sort":"server_order"},{"name":"online","by_columns":[],"text_params":[],"remote":{"operation":"PresenceOnline","shape":{"type":"list","coverage":"complete"},"stale_time":"30.seconds","empty_stale_time":null,"selects_page_info":false,"updates":[{"subscription":"PresenceChanged","deleted_by":"left","set":{},"removes":false,"debounce":null,"root":null,"id":null}],"page_size":null,"variables":{},"root":null},"is_global":true,"sort":"server_order"}],"composite_indexes":[],"actions":[],"ephemeral":[],"storage":"memory","expire_after":"1.minute","listens":[]},{"key":"User","fields":[{"name":"id","kind":{"type":"scalar","kind":"id"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null},{"name":"name","kind":{"type":"scalar","kind":"str"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null},{"name":"avatar","kind":{"type":"file"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null},{"name":"birthday","kind":{"type":"scalar","kind":"date"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null}],"associations":[],"relations":[{"name":"profile","by_columns":[],"text_params":[],"remote":{"operation":"UserProfile","shape":{"type":"single"},"stale_time":"5.minutes","empty_stale_time":null,"selects_page_info":false,"updates":[{"subscription":"UserTyping","deleted_by":null,"set":{"typing":true},"removes":false,"debounce":null,"root":null,"id":null}],"page_size":null,"variables":{},"root":null},"is_global":true,"sort":"server_order"}],"composite_indexes":[],"actions":[],"ephemeral":[{"name":"typing","kind":{"type":"scalar","kind":"bool"},"nullable":false,"optional":false,"default":false,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"reset_after":"5.seconds"},{"name":"mood","kind":{"type":"scalar","kind":"str"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"reset_after":null}],"storage":"disk","expire_after":null,"listens":[]},{"key":"Vibe","fields":[{"name":"id","kind":{"type":"scalar","kind":"id"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null},{"name":"momentId","kind":{"type":"scalar","kind":"id"},"nullable":false,"optional":false,"default":null,"indexed":true,"id_from_parent":null,"from":null,"app_type":null},{"name":"userId","kind":{"type":"scalar","kind":"id"},"nullable":false,"optional":false,"default":null,"indexed":true,"id_from_parent":null,"from":null,"app_type":null},{"name":"kind","kind":{"type":"enum","values":["like","love","laugh"]},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null}],"associations":[],"relations":[],"composite_indexes":[],"actions":[],"ephemeral":[],"storage":"disk","expire_after":null,"listens":[]}]"#
