import Foundation
import DBlayer

public struct Ledger: DBModel {
    public static let modelName = "Ledger"
    public let id: String
    public let scope: String
    public let sync: SyncInfo
    public let uploads: [String: UploadInfo]
    public let rowRevision: Int64
    public let estimatedBytes: UInt64

    public init(id: String, scope: String, sync: SyncInfo = SyncInfo(), uploads: [String: UploadInfo] = [:], rowRevision: Int64 = 0, estimatedBytes: UInt64 = 0) {
        self.id = id
        self.scope = scope
        self.sync = sync
        self.uploads = uploads
        self.rowRevision = rowRevision
        self.estimatedBytes = estimatedBytes
    }

    public static let wireLayout = "289c8b1976ed584f0fc8530e2c6a07072821088c740310c0619fbb37e245c33e"

    public init(wire row: WireRow) throws {
        let r = CellReader(row.cells)
        self.id = row.id
        self.scope = try r.req(0, \.asText)
        self.sync = SyncInfo(wire: row.sync)
        self.uploads = Dictionary(uniqueKeysWithValues: row.uploads.map { ($0.action, UploadInfo(wire: $0.upload)) })
        self.rowRevision = row.revision
        self.estimatedBytes = row.estimatedBytes
    }

    public static let scope = Column<Ledger, String>("scope")
    public static func `where`(_ condition: Condition) -> Query<Self> { Query<Self>().where(condition) }
    @concurrent public static func create(scope: String, in scope_: AccountScope? = nil) async throws -> MutationReceipt<Self> {
        var values_: [String: JSONValue] = [:]
        values_["scope"] = try JSONValue.encode(scope)
        return try await Self.create(values: values_, in: scope_)
    }
    @concurrent public func update(scope: String? = nil, in scope_: AccountScope? = nil) async throws -> MutationReceipt<Self> {
        var values_: [String: JSONValue] = [:]
        if let value = scope { values_["scope"] = try JSONValue.encode(value) }
        return try await self.update(values: values_, in: scope_)
    }
    @concurrent public func move(scope: String, in scope_: AccountScope? = nil) async throws -> MutationReceipt<Self> {
        let values_ = try JSONValue.readObject(#"{}"#)
        return try await self.act("move", values: values_, args: ["scope": try JSONValue.encode(scope)], in: scope_)
    }
}

public struct LedgerFeed: DBModel {
    public static let modelName = "LedgerFeed"
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

    public static let title = Column<LedgerFeed, String>("title")
    public static func `where`(_ condition: Condition) -> Query<Self> { Query<Self>().where(condition) }
    public static func entries(_ scope: String) throws -> Query<Self> {
        let built_ = Query<Self>(relation: "entries", params: ["scope": try JSONValue.encode(scope)])
        return built_
    }
    @concurrent public static func entriesEdge(_ scope: String, of id: String, in scope_: AccountScope? = nil) async throws -> LedgerFeedEntriesEdge? { try await Self.entries(scope).edgeValues(of: id, in: scope_).map(LedgerFeedEntriesEdge.init) }
    @concurrent public static func create(title: String, in scope_: AccountScope? = nil) async throws -> MutationReceipt<Self> {
        var values_: [String: JSONValue] = [:]
        values_["title"] = try JSONValue.encode(title)
        return try await Self.create(values: values_, in: scope_)
    }
    @concurrent public func update(title: String? = nil, in scope_: AccountScope? = nil) async throws -> MutationReceipt<Self> {
        var values_: [String: JSONValue] = [:]
        if let value = title { values_["title"] = try JSONValue.encode(value) }
        return try await self.update(values: values_, in: scope_)
    }
}

public struct Shadow: DBModel {
    public static let modelName = "Shadow"
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

    public static let title = Column<Shadow, String>("title")
    public static func `where`(_ condition: Condition) -> Query<Self> { Query<Self>().where(condition) }
    public static var scope: Query<Self> {
        let built_ = Query<Self>(relation: "scope", params: [:])
        return built_
    }
    @concurrent public static func scopeEdge(of id: String, in scope_: AccountScope? = nil) async throws -> ShadowScopeEdge? { try await Self.scope.edgeValues(of: id, in: scope_).map(ShadowScopeEdge.init) }
    public static var account: Query<Self> {
        let built_ = Query<Self>(relation: "account", params: [:])
        return built_
    }
    @concurrent public static func accountEdge(of id: String, in scope_: AccountScope? = nil) async throws -> ShadowAccountEdge? { try await Self.account.edgeValues(of: id, in: scope_).map(ShadowAccountEdge.init) }
    @concurrent public static func create(title: String, in scope_: AccountScope? = nil) async throws -> MutationReceipt<Self> {
        var values_: [String: JSONValue] = [:]
        values_["title"] = try JSONValue.encode(title)
        return try await Self.create(values: values_, in: scope_)
    }
    @concurrent public func update(title: String? = nil, in scope_: AccountScope? = nil) async throws -> MutationReceipt<Self> {
        var values_: [String: JSONValue] = [:]
        if let value = title { values_["title"] = try JSONValue.encode(value) }
        return try await self.update(values: values_, in: scope_)
    }
}

public struct Holder: DBSingleton {
    public static let modelName = "Holder"
    public let id: String
    public let label: String?
    public let sync: SyncInfo
    public let uploads: [String: UploadInfo]
    public let rowRevision: Int64
    public let estimatedBytes: UInt64

    public init(id: String, label: String? = nil, sync: SyncInfo = SyncInfo(), uploads: [String: UploadInfo] = [:], rowRevision: Int64 = 0, estimatedBytes: UInt64 = 0) {
        self.id = id
        self.label = label
        self.sync = sync
        self.uploads = uploads
        self.rowRevision = rowRevision
        self.estimatedBytes = estimatedBytes
    }

    public static let wireLayout = "cae70f3b4f7f7821a632570ad1a489bc597e5b848b1ccc6df664113173d98031"

    public init(wire row: WireRow) throws {
        let r = CellReader(row.cells)
        self.id = row.id
        self.label = try r.opt(0, \.asText)
        self.sync = SyncInfo(wire: row.sync)
        self.uploads = Dictionary(uniqueKeysWithValues: row.uploads.map { ($0.action, UploadInfo(wire: $0.upload)) })
        self.rowRevision = row.revision
        self.estimatedBytes = row.estimatedBytes
    }

    public static let label = Column<Holder, String?>("label")
    public static func `where`(_ condition: Condition) -> Query<Self> { Query<Self>().where(condition) }
    public static var account: Query<Self> {
        let built_ = Query<Self>(relation: "account", params: [:])
        return built_
    }
    public static var singleton: Query<Self> { Self.account }
    @concurrent public static func create(label: String? = nil, in scope_: AccountScope? = nil) async throws -> MutationReceipt<Self> {
        var values_: [String: JSONValue] = [:]
        if let value = label { values_["label"] = try JSONValue.encode(value) }
        return try await Self.create(values: values_, in: scope_)
    }
    @concurrent public func update(label: String?? = nil, in scope_: AccountScope? = nil) async throws -> MutationReceipt<Self> {
        var values_: [String: JSONValue] = [:]
        if let value = label { values_["label"] = try JSONValue.encode(value) }
        return try await self.update(values: values_, in: scope_)
    }
}

public struct Mailbox: DBModel {
    public static let modelName = "Mailbox"
    public let id: String
    public let read: Bool
    public let put: String?
    public let sync: SyncInfo
    public let uploads: [String: UploadInfo]
    public let rowRevision: Int64
    public let estimatedBytes: UInt64

    public init(id: String, read: Bool, put: String? = nil, sync: SyncInfo = SyncInfo(), uploads: [String: UploadInfo] = [:], rowRevision: Int64 = 0, estimatedBytes: UInt64 = 0) {
        self.id = id
        self.read = read
        self.put = put
        self.sync = sync
        self.uploads = uploads
        self.rowRevision = rowRevision
        self.estimatedBytes = estimatedBytes
    }

    public static let wireLayout = "378773461bb198bd38dc75a714a52c5eb2749b320835a2bec565b972cc490775"

    public init(wire row: WireRow) throws {
        let r = CellReader(row.cells)
        self.id = row.id
        self.read = try r.req(0, \.asBool)
        self.put = try r.opt(1, \.asText)
        self.sync = SyncInfo(wire: row.sync)
        self.uploads = Dictionary(uniqueKeysWithValues: row.uploads.map { ($0.action, UploadInfo(wire: $0.upload)) })
        self.rowRevision = row.revision
        self.estimatedBytes = row.estimatedBytes
    }

    public static let read = Column<Mailbox, Bool>("read")
    public static let put = Column<Mailbox, String?>("put")
    public static func `where`(_ condition: Condition) -> Query<Self> { Query<Self>().where(condition) }
    public static func entries(_ act: String) throws -> Query<Self> {
        let built_ = Query<Self>(relation: "entries", params: ["act": try JSONValue.encode(act)])
        return built_
    }
    @concurrent public static func entriesEdge(_ act: String, of id: String, in scope_: AccountScope? = nil) async throws -> MailboxEntriesEdge? { try await Self.entries(act).edgeValues(of: id, in: scope_).map(MailboxEntriesEdge.init) }
    @concurrent public static func create(read: Bool, put: String? = nil, in scope_: AccountScope? = nil) async throws -> MutationReceipt<Self> {
        var values_: [String: JSONValue] = [:]
        values_["read"] = try JSONValue.encode(read)
        if let value = put { values_["put"] = try JSONValue.encode(value) }
        return try await Self.create(values: values_, in: scope_)
    }
    @concurrent public func update(read: Bool? = nil, put: String?? = nil, in scope_: AccountScope? = nil) async throws -> MutationReceipt<Self> {
        var values_: [String: JSONValue] = [:]
        if let value = read { values_["read"] = try JSONValue.encode(value) }
        if let value = put { values_["put"] = try JSONValue.encode(value) }
        return try await self.update(values: values_, in: scope_)
    }
    @concurrent public func mark(put: String, in scope_: AccountScope? = nil) async throws -> MutationReceipt<Self> {
        let values_ = try JSONValue.readObject(#"{}"#)
        return try await self.act("mark", values: values_, args: ["put": try JSONValue.encode(put)], in: scope_)
    }
}

public struct Asset: DBModel {
    public static let modelName = "Asset"
    public let id: String
    public let clientKey: String
    public let size: Int
    public let position: Int
    public let sync: SyncInfo
    public let uploads: [String: UploadInfo]
    public let rowRevision: Int64
    public let estimatedBytes: UInt64

    public init(id: String, clientKey: String, size: Int, position: Int = 0, sync: SyncInfo = SyncInfo(), uploads: [String: UploadInfo] = [:], rowRevision: Int64 = 0, estimatedBytes: UInt64 = 0) {
        self.id = id
        self.clientKey = clientKey
        self.size = size
        self.position = position
        self.sync = sync
        self.uploads = uploads
        self.rowRevision = rowRevision
        self.estimatedBytes = estimatedBytes
    }

    public static let wireLayout = "c944299d26851922d3b1dce7742ce0d81a21d4035bc8de5b7c487960dc71565b"

    public init(wire row: WireRow) throws {
        let r = CellReader(row.cells)
        self.id = row.id
        self.clientKey = try r.req(0, \.asText)
        self.size = try r.req(1, \.asInt)
        self.position = try r.req(2, \.asInt)
        self.sync = SyncInfo(wire: row.sync)
        self.uploads = Dictionary(uniqueKeysWithValues: row.uploads.map { ($0.action, UploadInfo(wire: $0.upload)) })
        self.rowRevision = row.revision
        self.estimatedBytes = row.estimatedBytes
    }

    public static let clientKey = Column<Asset, String>("clientKey")
    public static let size = Column<Asset, Int>("size")
    public static let position = Column<Asset, Int>("position")
    public static func `where`(_ condition: Condition) -> Query<Self> { Query<Self>().where(condition) }
    @concurrent public static func create(clientKey: String, size: Int, position: Int? = nil, in scope_: AccountScope? = nil) async throws -> MutationReceipt<Self> {
        var values_: [String: JSONValue] = [:]
        values_["clientKey"] = try JSONValue.encode(clientKey)
        values_["size"] = try JSONValue.encode(size)
        if let value = position { values_["position"] = try JSONValue.encode(value) }
        return try await Self.create(values: values_, in: scope_)
    }
    @concurrent public func update(clientKey: String? = nil, size: Int? = nil, position: Int? = nil, in scope_: AccountScope? = nil) async throws -> MutationReceipt<Self> {
        var values_: [String: JSONValue] = [:]
        if let value = clientKey { values_["clientKey"] = try JSONValue.encode(value) }
        if let value = size { values_["size"] = try JSONValue.encode(value) }
        if let value = position { values_["position"] = try JSONValue.encode(value) }
        return try await self.update(values: values_, in: scope_)
    }
    @concurrent public func createFile(_ request: UploadRequest, in scope_: AccountScope) async throws -> MutationReceipt<Self> {
        try await scope_.upload(self, action: "create_file", request)
    }
}

public struct AssetCreateFileResult: Codable, Sendable {
}

public struct Club: DBModel {
    public static let modelName = "Club"
    public let id: String
    public let name: String
    public let sync: SyncInfo
    public let uploads: [String: UploadInfo]
    public let rowRevision: Int64
    public let estimatedBytes: UInt64

    public init(id: String, name: String, sync: SyncInfo = SyncInfo(), uploads: [String: UploadInfo] = [:], rowRevision: Int64 = 0, estimatedBytes: UInt64 = 0) {
        self.id = id
        self.name = name
        self.sync = sync
        self.uploads = uploads
        self.rowRevision = rowRevision
        self.estimatedBytes = estimatedBytes
    }

    public static let wireLayout = "daaaa5a3ca91ab534db47731be1fca27d87f8dd345d457bfd43bf77767339316"

    public init(wire row: WireRow) throws {
        let r = CellReader(row.cells)
        self.id = row.id
        self.name = try r.req(0, \.asText)
        self.sync = SyncInfo(wire: row.sync)
        self.uploads = Dictionary(uniqueKeysWithValues: row.uploads.map { ($0.action, UploadInfo(wire: $0.upload)) })
        self.rowRevision = row.revision
        self.estimatedBytes = row.estimatedBytes
    }

    public static let name = Column<Club, String>("name")
    public static func `where`(_ condition: Condition) -> Query<Self> { Query<Self>().where(condition) }
    @concurrent public static func create(name: String, in scope_: AccountScope? = nil) async throws -> MutationReceipt<Self> {
        var values_: [String: JSONValue] = [:]
        values_["name"] = try JSONValue.encode(name)
        return try await Self.create(values: values_, in: scope_)
    }
    @concurrent public func update(name: String? = nil, in scope_: AccountScope? = nil) async throws -> MutationReceipt<Self> {
        var values_: [String: JSONValue] = [:]
        if let value = name { values_["name"] = try JSONValue.encode(value) }
        return try await self.update(values: values_, in: scope_)
    }
    @concurrent public func leave(in scope_: AccountScope? = nil) async throws -> MutationReceipt<Self> {
        let values_ = try JSONValue.readObject(#"{}"#)
        return try await self.act("leave", values: values_, args: [:], in: scope_)
    }
    @concurrent public func addMember(userIds: [String], in scope_: AccountScope? = nil) async throws -> MutationReceipt<Self> {
        let values_ = try JSONValue.readObject(#"{}"#)
        return try await self.act("add_member", values: values_, args: ["userIds": try JSONValue.encode(userIds)], in: scope_)
    }
    @concurrent public func removeMember(userIds: [String], in scope_: AccountScope? = nil) async throws -> MutationReceipt<Self> {
        let values_ = try JSONValue.readObject(#"{}"#)
        return try await self.act("remove_member", values: values_, args: ["userIds": try JSONValue.encode(userIds)], in: scope_)
    }
}

public struct Person: DBModel {
    public static let modelName = "Person"
    public let id: String
    public let name: String
    public let sync: SyncInfo
    public let uploads: [String: UploadInfo]
    public let rowRevision: Int64
    public let estimatedBytes: UInt64

    public init(id: String, name: String, sync: SyncInfo = SyncInfo(), uploads: [String: UploadInfo] = [:], rowRevision: Int64 = 0, estimatedBytes: UInt64 = 0) {
        self.id = id
        self.name = name
        self.sync = sync
        self.uploads = uploads
        self.rowRevision = rowRevision
        self.estimatedBytes = estimatedBytes
    }

    public static let wireLayout = "daaaa5a3ca91ab534db47731be1fca27d87f8dd345d457bfd43bf77767339316"

    public init(wire row: WireRow) throws {
        let r = CellReader(row.cells)
        self.id = row.id
        self.name = try r.req(0, \.asText)
        self.sync = SyncInfo(wire: row.sync)
        self.uploads = Dictionary(uniqueKeysWithValues: row.uploads.map { ($0.action, UploadInfo(wire: $0.upload)) })
        self.rowRevision = row.revision
        self.estimatedBytes = row.estimatedBytes
    }

    public static let name = Column<Person, String>("name")
    public static func `where`(_ condition: Condition) -> Query<Self> { Query<Self>().where(condition) }
    public static func clubMembers(_ clubId: String, _ order: String) throws -> Query<Self> {
        let built_ = Query<Self>(relation: "club_members", params: ["clubId": try JSONValue.encode(clubId), "order": try JSONValue.encode(order)])
        return built_
    }
    @concurrent public static func create(name: String, in scope_: AccountScope? = nil) async throws -> MutationReceipt<Self> {
        var values_: [String: JSONValue] = [:]
        values_["name"] = try JSONValue.encode(name)
        return try await Self.create(values: values_, in: scope_)
    }
    @concurrent public func update(name: String? = nil, in scope_: AccountScope? = nil) async throws -> MutationReceipt<Self> {
        var values_: [String: JSONValue] = [:]
        if let value = name { values_["name"] = try JSONValue.encode(value) }
        return try await self.update(values: values_, in: scope_)
    }
}

public struct Letter: DBModel {
    public static let modelName = "Letter"
    public let id: String
    public let chatId: String
    public let text: String
    public let at: Int
    public let sync: SyncInfo
    public let uploads: [String: UploadInfo]
    public let rowRevision: Int64
    public let estimatedBytes: UInt64

    public init(id: String, chatId: String, text: String, at: Int, sync: SyncInfo = SyncInfo(), uploads: [String: UploadInfo] = [:], rowRevision: Int64 = 0, estimatedBytes: UInt64 = 0) {
        self.id = id
        self.chatId = chatId
        self.text = text
        self.at = at
        self.sync = sync
        self.uploads = uploads
        self.rowRevision = rowRevision
        self.estimatedBytes = estimatedBytes
    }

    public static let wireLayout = "c3472584e6f90c487fde5123abb7fd4d61c0787a5c6056261019759f11c496e7"

    public init(wire row: WireRow) throws {
        let r = CellReader(row.cells)
        self.id = row.id
        self.chatId = try r.req(0, \.asId)
        self.text = try r.req(1, \.asText)
        self.at = try r.req(2, \.asInt)
        self.sync = SyncInfo(wire: row.sync)
        self.uploads = Dictionary(uniqueKeysWithValues: row.uploads.map { ($0.action, UploadInfo(wire: $0.upload)) })
        self.rowRevision = row.revision
        self.estimatedBytes = row.estimatedBytes
    }

    public static let chatId = Column<Letter, String>("chatId")
    public static let text = Column<Letter, String>("text")
    public static let at = Column<Letter, Int>("at")
    public static func `where`(_ condition: Condition) -> Query<Self> { Query<Self>().where(condition) }
    public static func thread(_ chatId: String) throws -> Query<Self> {
        var built_ = Query<Self>(relation: "thread", params: ["chatId": try JSONValue.encode(chatId)])
        built_ = built_.where(Condition.equal("chatId", try JSONValue.encode(chatId)))
        built_ = built_.order(Order(field: "at", direction: .asc))
        return built_
    }
    @concurrent public static func create(chatId: String, text: String, at: Int, in scope_: AccountScope? = nil) async throws -> MutationReceipt<Self> {
        var values_: [String: JSONValue] = [:]
        values_["chatId"] = try JSONValue.encode(chatId)
        values_["text"] = try JSONValue.encode(text)
        values_["at"] = try JSONValue.encode(at)
        return try await Self.create(values: values_, in: scope_)
    }
    @concurrent public func update(chatId: String? = nil, text: String? = nil, at: Int? = nil, in scope_: AccountScope? = nil) async throws -> MutationReceipt<Self> {
        var values_: [String: JSONValue] = [:]
        if let value = chatId { values_["chatId"] = try JSONValue.encode(value) }
        if let value = text { values_["text"] = try JSONValue.encode(value) }
        if let value = at { values_["at"] = try JSONValue.encode(value) }
        return try await self.update(values: values_, in: scope_)
    }
    @concurrent public func forward(targetChatId: String, in scope_: AccountScope? = nil) async throws -> MutationReceipt<Self> {
        let values_ = try JSONValue.readObject(#"{}"#)
        return try await self.act("forward", values: values_, args: ["targetChatId": try JSONValue.encode(targetChatId)], in: scope_)
    }
}

public struct Container: DBModel {
    public static let modelName = "Container"
    public let id: String
    public let title: String
    public let lastRecordAt: String?
    public let lastRecordId: String?
    public let shareUrl: String?
    public let isPublic: Bool?
    public let shelves: [ContainerShelf]?
    public let sync: SyncInfo
    public let uploads: [String: UploadInfo]
    public let rowRevision: Int64
    public let estimatedBytes: UInt64

    public init(id: String, title: String, lastRecordAt: String? = nil, lastRecordId: String? = nil, shareUrl: String? = nil, isPublic: Bool? = nil, shelves: [ContainerShelf]? = nil, sync: SyncInfo = SyncInfo(), uploads: [String: UploadInfo] = [:], rowRevision: Int64 = 0, estimatedBytes: UInt64 = 0) {
        self.id = id
        self.title = title
        self.lastRecordAt = lastRecordAt
        self.lastRecordId = lastRecordId
        self.shareUrl = shareUrl
        self.isPublic = isPublic
        self.shelves = shelves
        self.sync = sync
        self.uploads = uploads
        self.rowRevision = rowRevision
        self.estimatedBytes = estimatedBytes
    }

    public static let wireLayout = "02f63dae390b644410b9064b71f41d994f1ae9f8b197e65a5899bac8ce7a1a39"

    public init(wire row: WireRow) throws {
        let r = CellReader(row.cells)
        self.id = row.id
        self.title = try r.req(0, \.asText)
        self.lastRecordAt = try r.opt(1, \.asText)
        self.lastRecordId = try r.opt(2, \.asId)
        self.shareUrl = try r.opt(3, \.asText)
        self.isPublic = try r.opt(4, \.asBool)
        self.shelves = try r.listOpt(5, \.asText)?.map(ContainerShelf.init(rawValue:))
        self.sync = SyncInfo(wire: row.sync)
        self.uploads = Dictionary(uniqueKeysWithValues: row.uploads.map { ($0.action, UploadInfo(wire: $0.upload)) })
        self.rowRevision = row.revision
        self.estimatedBytes = row.estimatedBytes
    }

    public static let title = Column<Container, String>("title")
    public static let lastRecordAt = Column<Container, String?>("lastRecordAt")
    public static let lastRecordId = Column<Container, String?>("lastRecordId")
    public static let shareUrl = Column<Container, String?>("shareUrl")
    public static let isPublic = Column<Container, Bool?>("isPublic")
    public static let shelves = Column<Container, [ContainerShelf]?>("shelves")
    public static func `where`(_ condition: Condition) -> Query<Self> { Query<Self>().where(condition) }
    @concurrent public static func node(_ id: String, force: Bool = false, in scope: AccountScope? = nil) async throws -> Self? {
        try await readNode(relation: "node", id: id, force: force, in: scope)
    }
    @concurrent public static func nodes(_ ids: [String], in scope: AccountScope? = nil) async throws -> [Self] {
        try await readNodes(relation: "nodes", ids: ids, in: scope)
    }
    public static var recent: Query<Self> {
        var built_ = Query<Self>(relation: "recent", params: [:])
        built_ = built_.order(Order(field: "lastRecordAt", direction: .desc))
        return built_
    }
    public static func shelf(shelf: ContainerShelf) throws -> Query<Self> {
        let built_ = Query<Self>(relation: "shelf", params: ["shelf": try JSONValue.encode(shelf)])
        return built_
    }
    @concurrent public static func create(title: String, lastRecordAt: String? = nil, lastRecordId: String? = nil, shareUrl: String? = nil, isPublic: Bool? = nil, shelves: [ContainerShelf]? = nil, in scope_: AccountScope? = nil) async throws -> MutationReceipt<Self> {
        var values_: [String: JSONValue] = [:]
        values_["title"] = try JSONValue.encode(title)
        if let value = lastRecordAt { values_["lastRecordAt"] = try JSONValue.encode(value) }
        if let value = lastRecordId { values_["lastRecordId"] = try JSONValue.encode(value) }
        if let value = shareUrl { values_["shareUrl"] = try JSONValue.encode(value) }
        if let value = isPublic { values_["isPublic"] = try JSONValue.encode(value) }
        if let value = shelves { values_["shelves"] = try JSONValue.encode(value) }
        return try await Self.create(values: values_, in: scope_)
    }
    @concurrent public func update(title: String? = nil, lastRecordAt: String?? = nil, lastRecordId: String?? = nil, shareUrl: String?? = nil, isPublic: Bool?? = nil, shelves: [ContainerShelf]?? = nil, in scope_: AccountScope? = nil) async throws -> MutationReceipt<Self> {
        var values_: [String: JSONValue] = [:]
        if let value = title { values_["title"] = try JSONValue.encode(value) }
        if let value = lastRecordAt { values_["lastRecordAt"] = try JSONValue.encode(value) }
        if let value = lastRecordId { values_["lastRecordId"] = try JSONValue.encode(value) }
        if let value = shareUrl { values_["shareUrl"] = try JSONValue.encode(value) }
        if let value = isPublic { values_["isPublic"] = try JSONValue.encode(value) }
        if let value = shelves { values_["shelves"] = try JSONValue.encode(value) }
        return try await self.update(values: values_, in: scope_)
    }
    public enum Relations {
        public static let records = Association<Container, Record>("records")
        public static let lastRecord = Association<Container, Record>("lastRecord")
    }
    @concurrent public func createPhoto(_ request: UploadRequest, in scope_: AccountScope) async throws -> MutationReceipt<Self> {
        try await scope_.upload(self, action: "create_photo", request)
    }
    @concurrent public func updatePhoto(_ request: UploadRequest, in scope_: AccountScope) async throws -> MutationReceipt<Self> {
        try await scope_.upload(self, action: "update_photo", request)
    }
}

public struct ContainerCreatePhotoResult: Codable, Sendable {
}

public struct ContainerUpdatePhotoResult: Codable, Sendable {
}

public struct Document: DBModel {
    public static let modelName = "Document"
    public let id: String
    public let slug: String
    public let sync: SyncInfo
    public let uploads: [String: UploadInfo]
    public let rowRevision: Int64
    public let estimatedBytes: UInt64

    public init(id: String, slug: String, sync: SyncInfo = SyncInfo(), uploads: [String: UploadInfo] = [:], rowRevision: Int64 = 0, estimatedBytes: UInt64 = 0) {
        self.id = id
        self.slug = slug
        self.sync = sync
        self.uploads = uploads
        self.rowRevision = rowRevision
        self.estimatedBytes = estimatedBytes
    }

    public static let wireLayout = "d615354dfb7622fbb88361a2ddb81628ff95c636cd10ab34ae33905bf9d3816d"

    public init(wire row: WireRow) throws {
        let r = CellReader(row.cells)
        self.id = row.id
        self.slug = try r.req(0, \.asText)
        self.sync = SyncInfo(wire: row.sync)
        self.uploads = Dictionary(uniqueKeysWithValues: row.uploads.map { ($0.action, UploadInfo(wire: $0.upload)) })
        self.rowRevision = row.revision
        self.estimatedBytes = row.estimatedBytes
    }

    public static let slug = Column<Document, String>("slug")
    public static func `where`(_ condition: Condition) -> Query<Self> { Query<Self>().where(condition) }
    public static func bySlug(_ slug: String, _ source: String) throws -> Query<Self> {
        let built_ = Query<Self>(relation: "bySlug", params: ["slug": try JSONValue.encode(slug), "source": try JSONValue.encode(source)])
        return built_
    }
    @concurrent public static func create(slug: String, in scope_: AccountScope? = nil) async throws -> MutationReceipt<Self> {
        var values_: [String: JSONValue] = [:]
        values_["slug"] = try JSONValue.encode(slug)
        return try await Self.create(values: values_, in: scope_)
    }
    @concurrent public func update(slug: String? = nil, in scope_: AccountScope? = nil) async throws -> MutationReceipt<Self> {
        var values_: [String: JSONValue] = [:]
        if let value = slug { values_["slug"] = try JSONValue.encode(value) }
        return try await self.update(values: values_, in: scope_)
    }
}

public struct Chat: DBModel {
    public static let modelName = "Chat"
    public let id: String
    public let title: String
    public let serverRevision: Int
    public let lists: [String]
    public let sync: SyncInfo
    public let uploads: [String: UploadInfo]
    public let rowRevision: Int64
    public let estimatedBytes: UInt64

    public init(id: String, title: String, serverRevision: Int, lists: [String], sync: SyncInfo = SyncInfo(), uploads: [String: UploadInfo] = [:], rowRevision: Int64 = 0, estimatedBytes: UInt64 = 0) {
        self.id = id
        self.title = title
        self.serverRevision = serverRevision
        self.lists = lists
        self.sync = sync
        self.uploads = uploads
        self.rowRevision = rowRevision
        self.estimatedBytes = estimatedBytes
    }

    public static let wireLayout = "b2e10e5dccda51fb5406718b018d2a6e51d4ce0f44de9b73f057b9f227ea29ed"

    public init(wire row: WireRow) throws {
        let r = CellReader(row.cells)
        self.id = row.id
        self.title = try r.req(0, \.asText)
        self.serverRevision = try r.req(1, \.asInt)
        self.lists = try r.list(2, \.asText)
        self.sync = SyncInfo(wire: row.sync)
        self.uploads = Dictionary(uniqueKeysWithValues: row.uploads.map { ($0.action, UploadInfo(wire: $0.upload)) })
        self.rowRevision = row.revision
        self.estimatedBytes = row.estimatedBytes
    }

    public static let title = Column<Chat, String>("title")
    public static let serverRevision = Column<Chat, Int>("serverRevision")
    public static let lists = Column<Chat, [String]>("lists")
    public static func `where`(_ condition: Condition) -> Query<Self> { Query<Self>().where(condition) }
    @concurrent public static func node(_ id: String, force: Bool = false, in scope: AccountScope? = nil) async throws -> Self? {
        try await readNode(relation: "node", id: id, force: force, in: scope)
    }
    public static func list(_ list: String) throws -> Query<Self> {
        let built_ = Query<Self>(relation: "list", params: ["list": try JSONValue.encode(list)])
        return built_
    }
    @concurrent public static func create(title: String, serverRevision: Int, lists: [String], in scope_: AccountScope? = nil) async throws -> MutationReceipt<Self> {
        var values_: [String: JSONValue] = [:]
        values_["title"] = try JSONValue.encode(title)
        values_["serverRevision"] = try JSONValue.encode(serverRevision)
        values_["lists"] = try JSONValue.encode(lists)
        return try await Self.create(values: values_, in: scope_)
    }
    @concurrent public func update(title: String? = nil, serverRevision: Int? = nil, lists: [String]? = nil, in scope_: AccountScope? = nil) async throws -> MutationReceipt<Self> {
        var values_: [String: JSONValue] = [:]
        if let value = title { values_["title"] = try JSONValue.encode(value) }
        if let value = serverRevision { values_["serverRevision"] = try JSONValue.encode(value) }
        if let value = lists { values_["lists"] = try JSONValue.encode(value) }
        return try await self.update(values: values_, in: scope_)
    }
}

public struct Message: DBModel {
    public static let modelName = "Message"
    public let id: String
    public let text: String
    public let chatId: String
    public let sync: SyncInfo
    public let uploads: [String: UploadInfo]
    public let rowRevision: Int64
    public let estimatedBytes: UInt64

    public init(id: String, text: String, chatId: String, sync: SyncInfo = SyncInfo(), uploads: [String: UploadInfo] = [:], rowRevision: Int64 = 0, estimatedBytes: UInt64 = 0) {
        self.id = id
        self.text = text
        self.chatId = chatId
        self.sync = sync
        self.uploads = uploads
        self.rowRevision = rowRevision
        self.estimatedBytes = estimatedBytes
    }

    public static let wireLayout = "7f04a6af8db05e525fca0f2c4ab814e68b7f988e351f5f29237e9671f3c26f53"

    public init(wire row: WireRow) throws {
        let r = CellReader(row.cells)
        self.id = row.id
        self.text = try r.req(0, \.asText)
        self.chatId = try r.req(1, \.asId)
        self.sync = SyncInfo(wire: row.sync)
        self.uploads = Dictionary(uniqueKeysWithValues: row.uploads.map { ($0.action, UploadInfo(wire: $0.upload)) })
        self.rowRevision = row.revision
        self.estimatedBytes = row.estimatedBytes
    }

    public static let text = Column<Message, String>("text")
    public static let chatId = Column<Message, String>("chatId")
    public static func `where`(_ condition: Condition) -> Query<Self> { Query<Self>().where(condition) }
    @concurrent public static func node(_ id: String, force: Bool = false, in scope: AccountScope? = nil) async throws -> Self? {
        try await readNode(relation: "node", id: id, force: force, in: scope)
    }
    @concurrent public static func create(text: String, chatId: String, in scope_: AccountScope? = nil) async throws -> MutationReceipt<Self> {
        var values_: [String: JSONValue] = [:]
        values_["text"] = try JSONValue.encode(text)
        values_["chatId"] = try JSONValue.encode(chatId)
        return try await Self.create(values: values_, in: scope_)
    }
    @concurrent public func update(text: String? = nil, chatId: String? = nil, in scope_: AccountScope? = nil) async throws -> MutationReceipt<Self> {
        var values_: [String: JSONValue] = [:]
        if let value = text { values_["text"] = try JSONValue.encode(value) }
        if let value = chatId { values_["chatId"] = try JSONValue.encode(value) }
        return try await self.update(values: values_, in: scope_)
    }
}

public struct InterfaceRecord: DBModel {
    public static let modelName = "InterfaceRecord"
    public let id: String
    public let slug: String
    public let spec: SampleSpec?
    public let sync: SyncInfo
    public let uploads: [String: UploadInfo]
    public let rowRevision: Int64
    public let estimatedBytes: UInt64

    public init(id: String, slug: String, spec: SampleSpec? = nil, sync: SyncInfo = SyncInfo(), uploads: [String: UploadInfo] = [:], rowRevision: Int64 = 0, estimatedBytes: UInt64 = 0) {
        self.id = id
        self.slug = slug
        self.spec = spec
        self.sync = sync
        self.uploads = uploads
        self.rowRevision = rowRevision
        self.estimatedBytes = estimatedBytes
    }

    public static let wireLayout = "d9411739ed10137bf5f5d850dcceb18d129a6b2b3944bb2081ccf6eef529886f"

    public init(wire row: WireRow) throws {
        let r = CellReader(row.cells)
        self.id = row.id
        self.slug = try r.req(0, \.asText)
        self.spec = try r.objOpt(1, SampleSpec.init(cells:))
        self.sync = SyncInfo(wire: row.sync)
        self.uploads = Dictionary(uniqueKeysWithValues: row.uploads.map { ($0.action, UploadInfo(wire: $0.upload)) })
        self.rowRevision = row.revision
        self.estimatedBytes = row.estimatedBytes
    }

    public static let slug = Column<InterfaceRecord, String>("slug")
    public static let spec = Column<InterfaceRecord, SampleSpec?>("spec")
    public static func `where`(_ condition: Condition) -> Query<Self> { Query<Self>().where(condition) }
    @concurrent public static func node(_ id: String, force: Bool = false, in scope: AccountScope? = nil) async throws -> Self? {
        try await readNode(relation: "node", id: id, force: force, in: scope)
    }
    @concurrent public static func create(slug: String, spec: SampleSpec? = nil, in scope_: AccountScope? = nil) async throws -> MutationReceipt<Self> {
        var values_: [String: JSONValue] = [:]
        values_["slug"] = try JSONValue.encode(slug)
        if let value = spec { values_["spec"] = try JSONValue.encode(value) }
        return try await Self.create(values: values_, in: scope_)
    }
    @concurrent public func update(slug: String? = nil, spec: SampleSpec?? = nil, in scope_: AccountScope? = nil) async throws -> MutationReceipt<Self> {
        var values_: [String: JSONValue] = [:]
        if let value = slug { values_["slug"] = try JSONValue.encode(value) }
        if let value = spec { values_["spec"] = try JSONValue.encode(value) }
        return try await self.update(values: values_, in: scope_)
    }
}

public struct KeywordRecord: DBModel {
    public static let modelName = "KeywordRecord"
    public let id: String
    public let `default`: String
    public let `class`: String
    public let value: KeywordValue?
    public let sync: SyncInfo
    public let uploads: [String: UploadInfo]
    public let rowRevision: Int64
    public let estimatedBytes: UInt64

    public init(id: String, `default`: String, `class`: String, value: KeywordValue? = nil, sync: SyncInfo = SyncInfo(), uploads: [String: UploadInfo] = [:], rowRevision: Int64 = 0, estimatedBytes: UInt64 = 0) {
        self.id = id
        self.`default` = `default`
        self.`class` = `class`
        self.value = value
        self.sync = sync
        self.uploads = uploads
        self.rowRevision = rowRevision
        self.estimatedBytes = estimatedBytes
    }

    public static let wireLayout = "30d8a50e4efa9241aaa8a14cc65f1e12b424397931dbff303f3268636176a5f4"

    public init(wire row: WireRow) throws {
        let r = CellReader(row.cells)
        self.id = row.id
        self.`default` = try r.req(0, \.asText)
        self.`class` = try r.req(1, \.asText)
        self.value = try r.objOpt(2, KeywordValue.init(cells:))
        self.sync = SyncInfo(wire: row.sync)
        self.uploads = Dictionary(uniqueKeysWithValues: row.uploads.map { ($0.action, UploadInfo(wire: $0.upload)) })
        self.rowRevision = row.revision
        self.estimatedBytes = row.estimatedBytes
    }

    public static let `default` = Column<KeywordRecord, String>("default")
    public static let `class` = Column<KeywordRecord, String>("class")
    public static let value = Column<KeywordRecord, KeywordValue?>("value")
    public static func `where`(_ condition: Condition) -> Query<Self> { Query<Self>().where(condition) }
    public static func when(_ `class`: String) throws -> Query<Self> {
        var built_ = Query<Self>(relation: "when", params: ["class": try JSONValue.encode(`class`)])
        built_ = built_.where(Condition.equal("class", try JSONValue.encode(`class`)))
        return built_
    }
    @concurrent public static func create(`default`: String, `class`: String, value: KeywordValue? = nil, in scope_: AccountScope? = nil) async throws -> MutationReceipt<Self> {
        var values_: [String: JSONValue] = [:]
        values_["default"] = try JSONValue.encode(`default`)
        values_["class"] = try JSONValue.encode(`class`)
        if let value = value { values_["value"] = try JSONValue.encode(value) }
        return try await Self.create(values: values_, in: scope_)
    }
    @concurrent public func update(`default`: String? = nil, `class`: String? = nil, value: KeywordValue?? = nil, in scope_: AccountScope? = nil) async throws -> MutationReceipt<Self> {
        var values_: [String: JSONValue] = [:]
        if let value = `default` { values_["default"] = try JSONValue.encode(value) }
        if let value = `class` { values_["class"] = try JSONValue.encode(value) }
        if let value = value { values_["value"] = try JSONValue.encode(value) }
        return try await self.update(values: values_, in: scope_)
    }
}

public struct MediaMessage: DBModel {
    public static let modelName = "MediaMessage"
    public let id: String
    public let chatId: String
    public let kind: MediaMessageKind
    public let mediaKind: MediaKindValue?
    public let sequenceNumber: Int?
    public let sync: SyncInfo
    public let uploads: [String: UploadInfo]
    public let rowRevision: Int64
    public let estimatedBytes: UInt64

    public init(id: String, chatId: String, kind: MediaMessageKind, mediaKind: MediaKindValue? = nil, sequenceNumber: Int? = nil, sync: SyncInfo = SyncInfo(), uploads: [String: UploadInfo] = [:], rowRevision: Int64 = 0, estimatedBytes: UInt64 = 0) {
        self.id = id
        self.chatId = chatId
        self.kind = kind
        self.mediaKind = mediaKind
        self.sequenceNumber = sequenceNumber
        self.sync = sync
        self.uploads = uploads
        self.rowRevision = rowRevision
        self.estimatedBytes = estimatedBytes
    }

    public static let wireLayout = "7d8a82402f1136ae53881e394710bc6c3cc3c11271b6d196ad4a7ba2f647c1c9"

    public init(wire row: WireRow) throws {
        let r = CellReader(row.cells)
        self.id = row.id
        self.chatId = try r.req(0, \.asId)
        self.kind = try MediaMessageKind(rawValue: r.req(1, \.asText))
        self.mediaKind = try r.opt(2, \.asText).map(MediaKindValue.init(rawValue:))
        self.sequenceNumber = try r.opt(3, \.asInt)
        self.sync = SyncInfo(wire: row.sync)
        self.uploads = Dictionary(uniqueKeysWithValues: row.uploads.map { ($0.action, UploadInfo(wire: $0.upload)) })
        self.rowRevision = row.revision
        self.estimatedBytes = row.estimatedBytes
    }

    public static let chatId = Column<MediaMessage, String>("chatId")
    public static let kind = Column<MediaMessage, MediaMessageKind>("kind")
    public static let mediaKind = Column<MediaMessage, MediaKindValue?>("mediaKind")
    public static let sequenceNumber = Column<MediaMessage, Int?>("sequenceNumber")
    public static func `where`(_ condition: Condition) -> Query<Self> { Query<Self>().where(condition) }
    public static func visualMedia(_ chatId: String) throws -> Query<Self> {
        let built_ = Query<Self>(relation: "visual_media", params: ["chatId": try JSONValue.encode(chatId)])
        return built_
    }
    public static func audioMedia(_ chatId: String) throws -> Query<Self> {
        let built_ = Query<Self>(relation: "audio_media", params: ["chatId": try JSONValue.encode(chatId)])
        return built_
    }
    public static func giftMedia(_ chatId: String) throws -> Query<Self> {
        let built_ = Query<Self>(relation: "gift_media", params: ["chatId": try JSONValue.encode(chatId)])
        return built_
    }
    public static func visualSince(_ chatId: String, afterSequence: Int) throws -> Query<Self> {
        let built_ = Query<Self>(relation: "visual_since", params: ["chatId": try JSONValue.encode(chatId), "afterSequence": try JSONValue.encode(afterSequence)])
        return built_
    }
    public static func audioSince(_ chatId: String, afterSequence: Int) throws -> Query<Self> {
        let built_ = Query<Self>(relation: "audio_since", params: ["chatId": try JSONValue.encode(chatId), "afterSequence": try JSONValue.encode(afterSequence)])
        return built_
    }
    public static func giftsSince(_ chatId: String, afterSequence: Int) throws -> Query<Self> {
        let built_ = Query<Self>(relation: "gifts_since", params: ["chatId": try JSONValue.encode(chatId), "afterSequence": try JSONValue.encode(afterSequence)])
        return built_
    }
    @concurrent public static func create(chatId: String, kind: MediaMessageKind, mediaKind: MediaKindValue? = nil, sequenceNumber: Int? = nil, in scope_: AccountScope? = nil) async throws -> MutationReceipt<Self> {
        var values_: [String: JSONValue] = [:]
        values_["chatId"] = try JSONValue.encode(chatId)
        values_["kind"] = try JSONValue.encode(kind)
        if let value = mediaKind { values_["mediaKind"] = try JSONValue.encode(value) }
        if let value = sequenceNumber { values_["sequenceNumber"] = try JSONValue.encode(value) }
        return try await Self.create(values: values_, in: scope_)
    }
    @concurrent public func update(chatId: String? = nil, kind: MediaMessageKind? = nil, mediaKind: MediaKindValue?? = nil, sequenceNumber: Int?? = nil, in scope_: AccountScope? = nil) async throws -> MutationReceipt<Self> {
        var values_: [String: JSONValue] = [:]
        if let value = chatId { values_["chatId"] = try JSONValue.encode(value) }
        if let value = kind { values_["kind"] = try JSONValue.encode(value) }
        if let value = mediaKind { values_["mediaKind"] = try JSONValue.encode(value) }
        if let value = sequenceNumber { values_["sequenceNumber"] = try JSONValue.encode(value) }
        return try await self.update(values: values_, in: scope_)
    }
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
    public let uploads: [String: UploadInfo]
    public let rowRevision: Int64
    public let estimatedBytes: UInt64

    public init(id: String, uuid: String, userId: String? = nil, text: String? = nil, likedByMe: Bool = false, media: JSONValue, tone: String? = nil, createdAt: String, sync: SyncInfo = SyncInfo(), uploads: [String: UploadInfo] = [:], rowRevision: Int64 = 0, estimatedBytes: UInt64 = 0) {
        self.id = id
        self.uuid = uuid
        self.userId = userId
        self.text = text
        self.likedByMe = likedByMe
        self.media = media
        self.tone = tone
        self.createdAt = createdAt
        self.sync = sync
        self.uploads = uploads
        self.rowRevision = rowRevision
        self.estimatedBytes = estimatedBytes
    }

    public static let wireLayout = "c505026a0e516c5ada9c1476fe85ce3827b542e9154608c489ed8729e13d96bc"

    public init(wire row: WireRow) throws {
        let r = CellReader(row.cells)
        self.id = row.id
        self.uuid = try r.req(0, \.asText)
        self.userId = try r.opt(1, \.asId)
        self.text = try r.opt(2, \.asText)
        self.likedByMe = try r.req(3, \.asBool)
        self.media = try r.json(4)
        self.tone = try r.opt(5, \.asText)
        self.createdAt = try r.req(6, \.asText)
        self.sync = SyncInfo(wire: row.sync)
        self.uploads = Dictionary(uniqueKeysWithValues: row.uploads.map { ($0.action, UploadInfo(wire: $0.upload)) })
        self.rowRevision = row.revision
        self.estimatedBytes = row.estimatedBytes
    }

    public static let uuid = Column<Moment, String>("uuid")
    public static let userId = Column<Moment, String?>("userId")
    public static let text = Column<Moment, String?>("text")
    public static let likedByMe = Column<Moment, Bool>("likedByMe")
    public static let media = Column<Moment, JSONValue>("media")
    public static let tone = Column<Moment, String?>("tone")
    public static let createdAt = Column<Moment, String>("createdAt")
    public static func `where`(_ condition: Condition) -> Query<Self> { Query<Self>().where(condition) }
    @concurrent public static func transcodeStatus(_ id: String, force: Bool = false, in scope: AccountScope? = nil) async throws -> Self? {
        try await readNode(relation: "transcode_status", id: id, force: force, in: scope)
    }
    public static var recent: Query<Self> {
        var built_ = Query<Self>(relation: "recent", params: [:])
        built_ = built_.order(Order(field: "createdAt", direction: .desc))
        return built_
    }
    public static func byUser(_ userId: String?) throws -> Query<Self> {
        var built_ = Query<Self>(relation: "by_user", params: ["userId": try JSONValue.encode(userId)])
        built_ = built_.where(Condition.equal("userId", try JSONValue.encode(userId)))
        built_ = built_.order(Order(field: "createdAt", direction: .desc))
        return built_
    }
    public static var feed: Query<Self> {
        let built_ = Query<Self>(relation: "feed", params: [:])
        return built_
    }
    @concurrent public static func feedEdge(of id: String, in scope_: AccountScope? = nil) async throws -> MomentFeedEdge? { try await Self.feed.edgeValues(of: id, in: scope_).map(MomentFeedEdge.init) }
    public static func vibeFeed(_ vibeId: String) throws -> Query<Self> {
        let built_ = Query<Self>(relation: "vibe_feed", params: ["vibeId": try JSONValue.encode(vibeId)])
        return built_
    }
    @concurrent public static func vibeFeedEdge(_ vibeId: String, of id: String, in scope_: AccountScope? = nil) async throws -> MomentVibeFeedEdge? { try await Self.vibeFeed(vibeId).edgeValues(of: id, in: scope_).map(MomentVibeFeedEdge.init) }
    @concurrent public static func create(text: String? = nil, media: JSONValue? = nil, in scope_: AccountScope? = nil) async throws -> MutationReceipt<Self> {
        var values_: [String: JSONValue] = [:]
        if let value = text { values_["text"] = try JSONValue.encode(value) }
        if let value = media { values_["media"] = try JSONValue.encode(value) }
        return try await Self.create(values: values_, in: scope_)
    }
    @concurrent public func update(uuid: String? = nil, userId: String?? = nil, text: String?? = nil, likedByMe: Bool? = nil, media: JSONValue? = nil, createdAt: String? = nil, in scope_: AccountScope? = nil) async throws -> MutationReceipt<Self> {
        var values_: [String: JSONValue] = [:]
        if let value = uuid { values_["uuid"] = try JSONValue.encode(value) }
        if let value = userId { values_["userId"] = try JSONValue.encode(value) }
        if let value = text { values_["text"] = try JSONValue.encode(value) }
        if let value = likedByMe { values_["likedByMe"] = try JSONValue.encode(value) }
        if let value = media { values_["media"] = try JSONValue.encode(value) }
        if let value = createdAt { values_["createdAt"] = try JSONValue.encode(value) }
        return try await self.update(values: values_, in: scope_)
    }
    public enum Relations {
        public static let author = Association<Moment, User>("author")
        public static let vibes = Association<Moment, Vibe>("vibes")
    }
    @concurrent public func like(in scope_: AccountScope? = nil) async throws -> MutationReceipt<Self> {
        let values_ = try JSONValue.readObject(#"{"likedByMe":true}"#)
        return try await self.act("like", values: values_, args: [:], in: scope_)
    }
    @concurrent public func share(in scope_: AccountScope? = nil) async throws -> MutationReceipt<Self> {
        let values_ = try JSONValue.readObject(#"{}"#)
        return try await self.act("share", values: values_, args: [:], in: scope_)
    }
    @concurrent public func uploadClip(_ request: UploadRequest, in scope_: AccountScope) async throws -> MutationReceipt<Self> {
        try await scope_.upload(self, action: "upload_clip", request)
    }
}

public struct MomentUploadClipResult: Codable, Sendable {
    public let successMessage: String?
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
    public let replyTo: RecordReplyNode?
    public let sync: SyncInfo
    public let uploads: [String: UploadInfo]
    public let rowRevision: Int64
    public let estimatedBytes: UInt64

    public init(id: String, phase: NestedSamplePhase? = nil, value: NestedValue? = nil, values: [NestedValue], tree: NestedTree? = nil, replyTo: RecordReplyNode? = nil, sync: SyncInfo = SyncInfo(), uploads: [String: UploadInfo] = [:], rowRevision: Int64 = 0, estimatedBytes: UInt64 = 0) {
        self.id = id
        self.phase = phase
        self.value = value
        self.values = values
        self.tree = tree
        self.replyTo = replyTo
        self.sync = sync
        self.uploads = uploads
        self.rowRevision = rowRevision
        self.estimatedBytes = estimatedBytes
    }

    public static let wireLayout = "52afdc8e805947d086c496697532378f7b168d3fdb0d9896be0740560f80b97c"

    public init(wire row: WireRow) throws {
        let r = CellReader(row.cells)
        self.id = row.id
        self.phase = try r.opt(0, \.asText).map(NestedSamplePhase.init(rawValue:))
        self.value = try r.objOpt(1, NestedValue.init(cells:))
        self.values = try r.arr(2, NestedValue.init(cells:))
        self.tree = try r.objOpt(3, NestedTree.init(cells:))
        self.replyTo = try r.objOpt(4, RecordReplyNode.init(cells:))
        self.sync = SyncInfo(wire: row.sync)
        self.uploads = Dictionary(uniqueKeysWithValues: row.uploads.map { ($0.action, UploadInfo(wire: $0.upload)) })
        self.rowRevision = row.revision
        self.estimatedBytes = row.estimatedBytes
    }

    public static let phase = Column<NestedSample, NestedSamplePhase?>("phase")
    public static let value = Column<NestedSample, NestedValue?>("value")
    public static let values = Column<NestedSample, [NestedValue]>("values")
    public static let tree = Column<NestedSample, NestedTree?>("tree")
    public static let replyTo = Column<NestedSample, RecordReplyNode?>("replyTo")
    public static func `where`(_ condition: Condition) -> Query<Self> { Query<Self>().where(condition) }
    @concurrent public static func create(phase: NestedSamplePhase? = nil, value: NestedValue? = nil, values: [NestedValue]? = nil, tree: NestedTree? = nil, replyTo: RecordReplyNode? = nil, in scope_: AccountScope? = nil) async throws -> MutationReceipt<Self> {
        var values_: [String: JSONValue] = [:]
        if let value = phase { values_["phase"] = try JSONValue.encode(value) }
        if let value = value { values_["value"] = try JSONValue.encode(value) }
        if let value = values { values_["values"] = try JSONValue.encode(value) }
        if let value = tree { values_["tree"] = try JSONValue.encode(value) }
        if let value = replyTo { values_["replyTo"] = try JSONValue.encode(value) }
        return try await Self.create(values: values_, in: scope_)
    }
    @concurrent public func update(phase: NestedSamplePhase?? = nil, value: NestedValue?? = nil, values: [NestedValue]? = nil, tree: NestedTree?? = nil, replyTo: RecordReplyNode?? = nil, in scope_: AccountScope? = nil) async throws -> MutationReceipt<Self> {
        var values_: [String: JSONValue] = [:]
        if let value = phase { values_["phase"] = try JSONValue.encode(value) }
        if let value = value { values_["value"] = try JSONValue.encode(value) }
        if let value = values { values_["values"] = try JSONValue.encode(value) }
        if let value = tree { values_["tree"] = try JSONValue.encode(value) }
        if let value = replyTo { values_["replyTo"] = try JSONValue.encode(value) }
        return try await self.update(values: values_, in: scope_)
    }
}

public struct NestedReference: DBModel {
    public static let modelName = "NestedReference"
    public let id: String
    public let values: [NestedValue]
    public let sync: SyncInfo
    public let uploads: [String: UploadInfo]
    public let rowRevision: Int64
    public let estimatedBytes: UInt64

    public init(id: String, values: [NestedValue], sync: SyncInfo = SyncInfo(), uploads: [String: UploadInfo] = [:], rowRevision: Int64 = 0, estimatedBytes: UInt64 = 0) {
        self.id = id
        self.values = values
        self.sync = sync
        self.uploads = uploads
        self.rowRevision = rowRevision
        self.estimatedBytes = estimatedBytes
    }

    public static let wireLayout = "97882d0dff35deb4c13aa698fda6e4caa3110d8b23342a0c15c10b29c64bd1ed"

    public init(wire row: WireRow) throws {
        let r = CellReader(row.cells)
        self.id = row.id
        self.values = try r.arr(0, NestedValue.init(cells:))
        self.sync = SyncInfo(wire: row.sync)
        self.uploads = Dictionary(uniqueKeysWithValues: row.uploads.map { ($0.action, UploadInfo(wire: $0.upload)) })
        self.rowRevision = row.revision
        self.estimatedBytes = row.estimatedBytes
    }

    public static let values = Column<NestedReference, [NestedValue]>("values")
    public static func `where`(_ condition: Condition) -> Query<Self> { Query<Self>().where(condition) }
    @concurrent public static func create(values: [NestedValue]? = nil, in scope_: AccountScope? = nil) async throws -> MutationReceipt<Self> {
        var values_: [String: JSONValue] = [:]
        if let value = values { values_["values"] = try JSONValue.encode(value) }
        return try await Self.create(values: values_, in: scope_)
    }
    @concurrent public func update(values: [NestedValue]? = nil, in scope_: AccountScope? = nil) async throws -> MutationReceipt<Self> {
        var values_: [String: JSONValue] = [:]
        if let value = values { values_["values"] = try JSONValue.encode(value) }
        return try await self.update(values: values_, in: scope_)
    }
}

public struct Notice: DBModel {
    public static let modelName = "Notice"
    public let id: String
    public let body: String
    public let occurredAt: String
    public let sync: SyncInfo
    public let uploads: [String: UploadInfo]
    public let rowRevision: Int64
    public let estimatedBytes: UInt64

    public init(id: String, body: String, occurredAt: String, sync: SyncInfo = SyncInfo(), uploads: [String: UploadInfo] = [:], rowRevision: Int64 = 0, estimatedBytes: UInt64 = 0) {
        self.id = id
        self.body = body
        self.occurredAt = occurredAt
        self.sync = sync
        self.uploads = uploads
        self.rowRevision = rowRevision
        self.estimatedBytes = estimatedBytes
    }

    public static let wireLayout = "6c5b254256db9ea92b4b3a01919bd0240688c7bcff6fc7962fab448a43277dac"

    public init(wire row: WireRow) throws {
        let r = CellReader(row.cells)
        self.id = row.id
        self.body = try r.req(0, \.asText)
        self.occurredAt = try r.req(1, \.asText)
        self.sync = SyncInfo(wire: row.sync)
        self.uploads = Dictionary(uniqueKeysWithValues: row.uploads.map { ($0.action, UploadInfo(wire: $0.upload)) })
        self.rowRevision = row.revision
        self.estimatedBytes = row.estimatedBytes
    }

    public static let body = Column<Notice, String>("body")
    public static let occurredAt = Column<Notice, String>("occurredAt")
    public static func `where`(_ condition: Condition) -> Query<Self> { Query<Self>().where(condition) }
    public static var recent: Query<Self> {
        var built_ = Query<Self>(relation: "recent", params: [:])
        built_ = built_.order(Order(field: "occurredAt", direction: .desc))
        return built_
    }
    @concurrent public static func create(body: String, occurredAt: String, in scope_: AccountScope? = nil) async throws -> MutationReceipt<Self> {
        var values_: [String: JSONValue] = [:]
        values_["body"] = try JSONValue.encode(body)
        values_["occurredAt"] = try JSONValue.encode(occurredAt)
        return try await Self.create(values: values_, in: scope_)
    }
    @concurrent public func update(body: String? = nil, occurredAt: String? = nil, in scope_: AccountScope? = nil) async throws -> MutationReceipt<Self> {
        var values_: [String: JSONValue] = [:]
        if let value = body { values_["body"] = try JSONValue.encode(value) }
        if let value = occurredAt { values_["occurredAt"] = try JSONValue.encode(value) }
        return try await self.update(values: values_, in: scope_)
    }
}

public struct InteractionNotice: DBModel {
    public static let modelName = "InteractionNotice"
    public let id: String
    public let recordId: String
    public let actorUserId: String
    public let occurredAt: String
    public let sync: SyncInfo
    public let uploads: [String: UploadInfo]
    public let rowRevision: Int64
    public let estimatedBytes: UInt64

    public init(id: String, recordId: String, actorUserId: String, occurredAt: String, sync: SyncInfo = SyncInfo(), uploads: [String: UploadInfo] = [:], rowRevision: Int64 = 0, estimatedBytes: UInt64 = 0) {
        self.id = id
        self.recordId = recordId
        self.actorUserId = actorUserId
        self.occurredAt = occurredAt
        self.sync = sync
        self.uploads = uploads
        self.rowRevision = rowRevision
        self.estimatedBytes = estimatedBytes
    }

    public static let wireLayout = "7ed7b3ff621f791d10e5925656859f01e34c46cb5c1a2bee390ff6f057db4b6a"

    public init(wire row: WireRow) throws {
        let r = CellReader(row.cells)
        self.id = row.id
        self.recordId = try r.req(0, \.asId)
        self.actorUserId = try r.req(1, \.asId)
        self.occurredAt = try r.req(2, \.asText)
        self.sync = SyncInfo(wire: row.sync)
        self.uploads = Dictionary(uniqueKeysWithValues: row.uploads.map { ($0.action, UploadInfo(wire: $0.upload)) })
        self.rowRevision = row.revision
        self.estimatedBytes = row.estimatedBytes
    }

    public static let recordId = Column<InteractionNotice, String>("recordId")
    public static let actorUserId = Column<InteractionNotice, String>("actorUserId")
    public static let occurredAt = Column<InteractionNotice, String>("occurredAt")
    public static func `where`(_ condition: Condition) -> Query<Self> { Query<Self>().where(condition) }
    public static var recent: Query<Self> {
        var built_ = Query<Self>(relation: "recent", params: [:])
        built_ = built_.order(Order(field: "occurredAt", direction: .desc))
        return built_
    }
    @concurrent public static func create(recordId: String, actorUserId: String, occurredAt: String, in scope_: AccountScope? = nil) async throws -> MutationReceipt<Self> {
        var values_: [String: JSONValue] = [:]
        values_["recordId"] = try JSONValue.encode(recordId)
        values_["actorUserId"] = try JSONValue.encode(actorUserId)
        values_["occurredAt"] = try JSONValue.encode(occurredAt)
        return try await Self.create(values: values_, in: scope_)
    }
    @concurrent public func update(recordId: String? = nil, actorUserId: String? = nil, occurredAt: String? = nil, in scope_: AccountScope? = nil) async throws -> MutationReceipt<Self> {
        var values_: [String: JSONValue] = [:]
        if let value = recordId { values_["recordId"] = try JSONValue.encode(value) }
        if let value = actorUserId { values_["actorUserId"] = try JSONValue.encode(value) }
        if let value = occurredAt { values_["occurredAt"] = try JSONValue.encode(value) }
        return try await self.update(values: values_, in: scope_)
    }
}

public struct ChatAction: DBModel {
    public static let modelName = "ChatAction"
    public let id: String
    public let chatId: String
    public let userId: String
    public let kind: String
    public let active: Bool
    public let sync: SyncInfo
    public let uploads: [String: UploadInfo]
    public let rowRevision: Int64
    public let estimatedBytes: UInt64

    public init(id: String, chatId: String, userId: String, kind: String, active: Bool, sync: SyncInfo = SyncInfo(), uploads: [String: UploadInfo] = [:], rowRevision: Int64 = 0, estimatedBytes: UInt64 = 0) {
        self.id = id
        self.chatId = chatId
        self.userId = userId
        self.kind = kind
        self.active = active
        self.sync = sync
        self.uploads = uploads
        self.rowRevision = rowRevision
        self.estimatedBytes = estimatedBytes
    }

    public static let wireLayout = "b55e02f22bda54283d7380d5f2aa30ed868acba6e775e077cf6f1b2e1b956caf"

    public init(wire row: WireRow) throws {
        let r = CellReader(row.cells)
        self.id = row.id
        self.chatId = try r.req(0, \.asId)
        self.userId = try r.req(1, \.asId)
        self.kind = try r.req(2, \.asText)
        self.active = try r.req(3, \.asBool)
        self.sync = SyncInfo(wire: row.sync)
        self.uploads = Dictionary(uniqueKeysWithValues: row.uploads.map { ($0.action, UploadInfo(wire: $0.upload)) })
        self.rowRevision = row.revision
        self.estimatedBytes = row.estimatedBytes
    }

    public static let chatId = Column<ChatAction, String>("chatId")
    public static let userId = Column<ChatAction, String>("userId")
    public static let kind = Column<ChatAction, String>("kind")
    public static let active = Column<ChatAction, Bool>("active")
    public static func `where`(_ condition: Condition) -> Query<Self> { Query<Self>().where(condition) }
    public static func byChat(_ chatId: String) throws -> Query<Self> {
        var built_ = Query<Self>(relation: "by_chat", params: ["chatId": try JSONValue.encode(chatId)])
        built_ = built_.where(Condition.equal("chatId", try JSONValue.encode(chatId)))
        return built_
    }
    @concurrent public static func create(chatId: String, userId: String, kind: String, active: Bool, in scope_: AccountScope? = nil) async throws -> MutationReceipt<Self> {
        var values_: [String: JSONValue] = [:]
        values_["chatId"] = try JSONValue.encode(chatId)
        values_["userId"] = try JSONValue.encode(userId)
        values_["kind"] = try JSONValue.encode(kind)
        values_["active"] = try JSONValue.encode(active)
        return try await Self.create(values: values_, in: scope_)
    }
    @concurrent public func update(chatId: String? = nil, userId: String? = nil, kind: String? = nil, active: Bool? = nil, in scope_: AccountScope? = nil) async throws -> MutationReceipt<Self> {
        var values_: [String: JSONValue] = [:]
        if let value = chatId { values_["chatId"] = try JSONValue.encode(value) }
        if let value = userId { values_["userId"] = try JSONValue.encode(value) }
        if let value = kind { values_["kind"] = try JSONValue.encode(value) }
        if let value = active { values_["active"] = try JSONValue.encode(value) }
        return try await self.update(values: values_, in: scope_)
    }
}

public struct ChatActivityNotice: DBModel {
    public static let modelName = "ChatActivityNotice"
    public let id: String
    public let chatId: String
    public let userId: String
    public let kind: String
    public let active: Bool
    public let sync: SyncInfo
    public let uploads: [String: UploadInfo]
    public let rowRevision: Int64
    public let estimatedBytes: UInt64

    public init(id: String, chatId: String, userId: String, kind: String, active: Bool, sync: SyncInfo = SyncInfo(), uploads: [String: UploadInfo] = [:], rowRevision: Int64 = 0, estimatedBytes: UInt64 = 0) {
        self.id = id
        self.chatId = chatId
        self.userId = userId
        self.kind = kind
        self.active = active
        self.sync = sync
        self.uploads = uploads
        self.rowRevision = rowRevision
        self.estimatedBytes = estimatedBytes
    }

    public static let wireLayout = "b55e02f22bda54283d7380d5f2aa30ed868acba6e775e077cf6f1b2e1b956caf"

    public init(wire row: WireRow) throws {
        let r = CellReader(row.cells)
        self.id = row.id
        self.chatId = try r.req(0, \.asId)
        self.userId = try r.req(1, \.asId)
        self.kind = try r.req(2, \.asText)
        self.active = try r.req(3, \.asBool)
        self.sync = SyncInfo(wire: row.sync)
        self.uploads = Dictionary(uniqueKeysWithValues: row.uploads.map { ($0.action, UploadInfo(wire: $0.upload)) })
        self.rowRevision = row.revision
        self.estimatedBytes = row.estimatedBytes
    }

    public static let chatId = Column<ChatActivityNotice, String>("chatId")
    public static let userId = Column<ChatActivityNotice, String>("userId")
    public static let kind = Column<ChatActivityNotice, String>("kind")
    public static let active = Column<ChatActivityNotice, Bool>("active")
    public static func `where`(_ condition: Condition) -> Query<Self> { Query<Self>().where(condition) }
    public static var recent: Query<Self> {
        var built_ = Query<Self>(relation: "recent", params: [:])
        built_ = built_.order(Order(field: "kind", direction: .asc))
        return built_
    }
    @concurrent public static func create(chatId: String, userId: String, kind: String, active: Bool, in scope_: AccountScope? = nil) async throws -> MutationReceipt<Self> {
        var values_: [String: JSONValue] = [:]
        values_["chatId"] = try JSONValue.encode(chatId)
        values_["userId"] = try JSONValue.encode(userId)
        values_["kind"] = try JSONValue.encode(kind)
        values_["active"] = try JSONValue.encode(active)
        return try await Self.create(values: values_, in: scope_)
    }
    @concurrent public func update(chatId: String? = nil, userId: String? = nil, kind: String? = nil, active: Bool? = nil, in scope_: AccountScope? = nil) async throws -> MutationReceipt<Self> {
        var values_: [String: JSONValue] = [:]
        if let value = chatId { values_["chatId"] = try JSONValue.encode(value) }
        if let value = userId { values_["userId"] = try JSONValue.encode(value) }
        if let value = kind { values_["kind"] = try JSONValue.encode(value) }
        if let value = active { values_["active"] = try JSONValue.encode(value) }
        return try await self.update(values: values_, in: scope_)
    }
}

public struct Presence: DBModel {
    public static let modelName = "Presence"
    public let id: String
    public let userId: String
    public let room: String
    public let sync: SyncInfo
    public let uploads: [String: UploadInfo]
    public let rowRevision: Int64
    public let estimatedBytes: UInt64

    public init(id: String, userId: String, room: String, sync: SyncInfo = SyncInfo(), uploads: [String: UploadInfo] = [:], rowRevision: Int64 = 0, estimatedBytes: UInt64 = 0) {
        self.id = id
        self.userId = userId
        self.room = room
        self.sync = sync
        self.uploads = uploads
        self.rowRevision = rowRevision
        self.estimatedBytes = estimatedBytes
    }

    public static let wireLayout = "6389b939e12075c3b9a36a258c0ebb41dee76a6ce786ccf5d21f28dad18bfc97"

    public init(wire row: WireRow) throws {
        let r = CellReader(row.cells)
        self.id = row.id
        self.userId = try r.req(0, \.asId)
        self.room = try r.req(1, \.asText)
        self.sync = SyncInfo(wire: row.sync)
        self.uploads = Dictionary(uniqueKeysWithValues: row.uploads.map { ($0.action, UploadInfo(wire: $0.upload)) })
        self.rowRevision = row.revision
        self.estimatedBytes = row.estimatedBytes
    }

    public static let userId = Column<Presence, String>("userId")
    public static let room = Column<Presence, String>("room")
    public static func `where`(_ condition: Condition) -> Query<Self> { Query<Self>().where(condition) }
    public static func forUser(_ userId: String) throws -> Query<Self> {
        var built_ = Query<Self>(relation: "for_user", params: ["userId": try JSONValue.encode(userId)])
        built_ = built_.where(Condition.equal("userId", try JSONValue.encode(userId)))
        return built_
    }
    public static var online: Query<Self> {
        let built_ = Query<Self>(relation: "online", params: [:])
        return built_
    }
    public static func onChannel(_ channel: String) throws -> Query<Self> {
        let built_ = Query<Self>(relation: "on_channel", params: ["channel": try JSONValue.encode(channel)])
        return built_
    }
    public static func history(_ presenceId: String) throws -> Query<Self> {
        let built_ = Query<Self>(relation: "history", params: ["presenceId": try JSONValue.encode(presenceId)])
        return built_
    }
    @concurrent public static func create(userId: String, room: String, in scope_: AccountScope? = nil) async throws -> MutationReceipt<Self> {
        var values_: [String: JSONValue] = [:]
        values_["userId"] = try JSONValue.encode(userId)
        values_["room"] = try JSONValue.encode(room)
        return try await Self.create(values: values_, in: scope_)
    }
    @concurrent public func update(userId: String? = nil, room: String? = nil, in scope_: AccountScope? = nil) async throws -> MutationReceipt<Self> {
        var values_: [String: JSONValue] = [:]
        if let value = userId { values_["userId"] = try JSONValue.encode(value) }
        if let value = room { values_["room"] = try JSONValue.encode(value) }
        return try await self.update(values: values_, in: scope_)
    }
    public enum Relations {
        public static let user = Association<Presence, User>("user")
    }
}

public struct Record: DBModel {
    public static let modelName = "Record"
    public let id: String
    public let containerId: String
    public let authorId: String
    public let regionId: String?
    public let text: String
    public let createdAt: String
    public let sequenceNumber: Int?
    public let reactions: [RecordReactionNode]
    public let attachment: RecordAttachment?
    public let media: RecordMedia?
    public let sync: SyncInfo
    public let uploads: [String: UploadInfo]
    public let rowRevision: Int64
    public let estimatedBytes: UInt64

    public init(id: String, containerId: String, authorId: String, regionId: String? = nil, text: String, createdAt: String, sequenceNumber: Int? = nil, reactions: [RecordReactionNode], attachment: RecordAttachment? = nil, media: RecordMedia? = nil, sync: SyncInfo = SyncInfo(), uploads: [String: UploadInfo] = [:], rowRevision: Int64 = 0, estimatedBytes: UInt64 = 0) {
        self.id = id
        self.containerId = containerId
        self.authorId = authorId
        self.regionId = regionId
        self.text = text
        self.createdAt = createdAt
        self.sequenceNumber = sequenceNumber
        self.reactions = reactions
        self.attachment = attachment
        self.media = media
        self.sync = sync
        self.uploads = uploads
        self.rowRevision = rowRevision
        self.estimatedBytes = estimatedBytes
    }

    public static let wireLayout = "33f44335aa00c05718e2368d6acb029ee8c5347b5113f9ee3cc8a5c394629c52"

    public init(wire row: WireRow) throws {
        let r = CellReader(row.cells)
        self.id = row.id
        self.containerId = try r.req(0, \.asId)
        self.authorId = try r.req(1, \.asId)
        self.regionId = try r.opt(2, \.asId)
        self.text = try r.req(3, \.asText)
        self.createdAt = try r.req(4, \.asText)
        self.sequenceNumber = try r.opt(5, \.asInt)
        self.reactions = try r.arr(6, RecordReactionNode.init(cells:))
        self.attachment = try r.objOpt(7, RecordAttachment.init(cells:))
        self.media = try r.objOpt(8, RecordMedia.init(cells:))
        self.sync = SyncInfo(wire: row.sync)
        self.uploads = Dictionary(uniqueKeysWithValues: row.uploads.map { ($0.action, UploadInfo(wire: $0.upload)) })
        self.rowRevision = row.revision
        self.estimatedBytes = row.estimatedBytes
    }

    public static let containerId = Column<Record, String>("containerId")
    public static let authorId = Column<Record, String>("authorId")
    public static let regionId = Column<Record, String?>("regionId")
    public static let text = Column<Record, String>("text")
    public static let createdAt = Column<Record, String>("createdAt")
    public static let sequenceNumber = Column<Record, Int?>("sequenceNumber")
    public static let reactions = Column<Record, [RecordReactionNode]>("reactions")
    public static let attachment = Column<Record, RecordAttachmentKind>("attachment")
    public static let media = Column<Record, RecordMedia?>("media")
    public static func `where`(_ condition: Condition) -> Query<Self> { Query<Self>().where(condition) }
    public static func byContainer(_ containerId: String) throws -> Query<Self> {
        var built_ = Query<Self>(relation: "by_container", params: ["containerId": try JSONValue.encode(containerId)])
        built_ = built_.where(Condition.equal("containerId", try JSONValue.encode(containerId)))
        built_ = built_.order(Order(field: "createdAt", direction: .asc))
        return built_
    }
    public static func thread(_ containerId: String) throws -> Query<Self> {
        let built_ = Query<Self>(relation: "thread", params: ["containerId": try JSONValue.encode(containerId)])
        return built_
    }
    @concurrent public static func node(_ id: String, force: Bool = false, in scope: AccountScope? = nil) async throws -> Self? {
        try await readNode(relation: "node", id: id, force: force, in: scope)
    }
    @concurrent public static func transcodeStatus(_ id: String, force: Bool = false, in scope: AccountScope? = nil) async throws -> Self? {
        try await readNode(relation: "transcode_status", id: id, force: force, in: scope)
    }
    public static func containerMedia(_ containerId: String) throws -> Query<Self> {
        let built_ = Query<Self>(relation: "container_media", params: ["containerId": try JSONValue.encode(containerId)])
        return built_
    }
    public static var history: Query<Self> {
        let built_ = Query<Self>(relation: "history", params: [:])
        return built_
    }
    @concurrent public static func create(containerId: String, authorId: String? = nil, regionId: String? = nil, text: String, attachedUserId: String? = nil, signedId: String? = nil, in scope_: AccountScope? = nil) async throws -> MutationReceipt<Self> {
        var values_: [String: JSONValue] = [:]
        var args_: [String: JSONValue] = [:]
        values_["containerId"] = try JSONValue.encode(containerId)
        if let value = authorId { values_["authorId"] = try JSONValue.encode(value) }
        if let value = regionId { values_["regionId"] = try JSONValue.encode(value) }
        values_["text"] = try JSONValue.encode(text)
        if let value = attachedUserId { args_["attachedUserId"] = try JSONValue.encode(value) }
        if let value = signedId { args_["signedId"] = try JSONValue.encode(value) }
        return try await Self.create(values: values_, args: args_, in: scope_)
    }
    @concurrent public func update(containerId: String? = nil, authorId: String? = nil, regionId: String?? = nil, text: String? = nil, createdAt: String? = nil, sequenceNumber: Int?? = nil, reactions: [RecordReactionNode]? = nil, attachment: RecordAttachment?? = nil, media: RecordMedia?? = nil, in scope_: AccountScope? = nil) async throws -> MutationReceipt<Self> {
        var values_: [String: JSONValue] = [:]
        if let value = containerId { values_["containerId"] = try JSONValue.encode(value) }
        if let value = authorId { values_["authorId"] = try JSONValue.encode(value) }
        if let value = regionId { values_["regionId"] = try JSONValue.encode(value) }
        if let value = text { values_["text"] = try JSONValue.encode(value) }
        if let value = createdAt { values_["createdAt"] = try JSONValue.encode(value) }
        if let value = sequenceNumber { values_["sequenceNumber"] = try JSONValue.encode(value) }
        if let value = reactions { values_["reactions"] = try JSONValue.encode(value) }
        if let value = attachment { values_["attachment"] = try JSONValue.encode(value) }
        if let value = media { values_["media"] = try JSONValue.encode(value) }
        return try await self.update(values: values_, in: scope_)
    }
    public enum Relations {
        public static let container = Association<Record, Container>("container")
    }
    @concurrent public func react(reactionBody: String, userId: String, in scope_: AccountScope? = nil) async throws -> MutationReceipt<Self> {
        let values_ = try JSONValue.readObject(#"{"reactions":{"$keyed":{"by":"userId","element":{"body":{"$arg":"reactionBody"},"userId":{"$arg":"userId"}}}}}"#)
        return try await self.act("react", values: values_, args: ["reactionBody": try JSONValue.encode(reactionBody), "userId": try JSONValue.encode(userId)], in: scope_)
    }
}

public struct CurrentUser: DBSingleton {
    public static let modelName = "CurrentUser"
    public let id: String
    public let name: String
    public let sync: SyncInfo
    public let uploads: [String: UploadInfo]
    public let rowRevision: Int64
    public let estimatedBytes: UInt64

    public init(id: String, name: String, sync: SyncInfo = SyncInfo(), uploads: [String: UploadInfo] = [:], rowRevision: Int64 = 0, estimatedBytes: UInt64 = 0) {
        self.id = id
        self.name = name
        self.sync = sync
        self.uploads = uploads
        self.rowRevision = rowRevision
        self.estimatedBytes = estimatedBytes
    }

    public static let wireLayout = "daaaa5a3ca91ab534db47731be1fca27d87f8dd345d457bfd43bf77767339316"

    public init(wire row: WireRow) throws {
        let r = CellReader(row.cells)
        self.id = row.id
        self.name = try r.req(0, \.asText)
        self.sync = SyncInfo(wire: row.sync)
        self.uploads = Dictionary(uniqueKeysWithValues: row.uploads.map { ($0.action, UploadInfo(wire: $0.upload)) })
        self.rowRevision = row.revision
        self.estimatedBytes = row.estimatedBytes
    }

    public static let name = Column<CurrentUser, String>("name")
    public static func `where`(_ condition: Condition) -> Query<Self> { Query<Self>().where(condition) }
    public static var current: Query<Self> {
        let built_ = Query<Self>(relation: "current", params: [:])
        return built_
    }
    public static var singleton: Query<Self> { Self.current }
    @concurrent public static func create(name: String, in scope_: AccountScope? = nil) async throws -> MutationReceipt<Self> {
        var values_: [String: JSONValue] = [:]
        values_["name"] = try JSONValue.encode(name)
        return try await Self.create(values: values_, in: scope_)
    }
    @concurrent public func update(name: String? = nil, in scope_: AccountScope? = nil) async throws -> MutationReceipt<Self> {
        var values_: [String: JSONValue] = [:]
        if let value = name { values_["name"] = try JSONValue.encode(value) }
        return try await self.update(values: values_, in: scope_)
    }
}

public struct CurrentRegion: DBSingleton {
    public static let modelName = "CurrentRegion"
    public let id: String
    public let name: String
    public let sync: SyncInfo
    public let uploads: [String: UploadInfo]
    public let rowRevision: Int64
    public let estimatedBytes: UInt64

    public init(id: String, name: String, sync: SyncInfo = SyncInfo(), uploads: [String: UploadInfo] = [:], rowRevision: Int64 = 0, estimatedBytes: UInt64 = 0) {
        self.id = id
        self.name = name
        self.sync = sync
        self.uploads = uploads
        self.rowRevision = rowRevision
        self.estimatedBytes = estimatedBytes
    }

    public static let wireLayout = "daaaa5a3ca91ab534db47731be1fca27d87f8dd345d457bfd43bf77767339316"

    public init(wire row: WireRow) throws {
        let r = CellReader(row.cells)
        self.id = row.id
        self.name = try r.req(0, \.asText)
        self.sync = SyncInfo(wire: row.sync)
        self.uploads = Dictionary(uniqueKeysWithValues: row.uploads.map { ($0.action, UploadInfo(wire: $0.upload)) })
        self.rowRevision = row.revision
        self.estimatedBytes = row.estimatedBytes
    }

    public static let name = Column<CurrentRegion, String>("name")
    public static func `where`(_ condition: Condition) -> Query<Self> { Query<Self>().where(condition) }
    public static var current: Query<Self> {
        let built_ = Query<Self>(relation: "current", params: [:])
        return built_
    }
    public static var singleton: Query<Self> { Self.current }
    @concurrent public static func create(name: String, in scope_: AccountScope? = nil) async throws -> MutationReceipt<Self> {
        var values_: [String: JSONValue] = [:]
        values_["name"] = try JSONValue.encode(name)
        return try await Self.create(values: values_, in: scope_)
    }
    @concurrent public func update(name: String? = nil, in scope_: AccountScope? = nil) async throws -> MutationReceipt<Self> {
        var values_: [String: JSONValue] = [:]
        if let value = name { values_["name"] = try JSONValue.encode(value) }
        return try await self.update(values: values_, in: scope_)
    }
}

public struct CustomScreen: DBModel {
    public static let modelName = "CustomScreen"
    public let id: String
    public let slug: String
    public let version: Int
    public let requires: ScreenRequires
    public let spec: ScreenSpec?
    public let sync: SyncInfo
    public let uploads: [String: UploadInfo]
    public let rowRevision: Int64
    public let estimatedBytes: UInt64

    public init(id: String, slug: String, version: Int, requires: ScreenRequires, spec: ScreenSpec? = nil, sync: SyncInfo = SyncInfo(), uploads: [String: UploadInfo] = [:], rowRevision: Int64 = 0, estimatedBytes: UInt64 = 0) {
        self.id = id
        self.slug = slug
        self.version = version
        self.requires = requires
        self.spec = spec
        self.sync = sync
        self.uploads = uploads
        self.rowRevision = rowRevision
        self.estimatedBytes = estimatedBytes
    }

    public static let wireLayout = "3e13f0e795a7c1d99a23f73a144dd682d8dd7c01b156079f8dc93ce6122c9129"

    public init(wire row: WireRow) throws {
        let r = CellReader(row.cells)
        self.id = row.id
        self.slug = try r.req(0, \.asText)
        self.version = try r.req(1, \.asInt)
        self.requires = try r.obj(2, ScreenRequires.init(cells:))
        self.spec = try r.objOpt(3, ScreenSpec.init(cells:))
        self.sync = SyncInfo(wire: row.sync)
        self.uploads = Dictionary(uniqueKeysWithValues: row.uploads.map { ($0.action, UploadInfo(wire: $0.upload)) })
        self.rowRevision = row.revision
        self.estimatedBytes = row.estimatedBytes
    }

    public static let slug = Column<CustomScreen, String>("slug")
    public static let version = Column<CustomScreen, Int>("version")
    public static let requires = Column<CustomScreen, ScreenRequires>("requires")
    public static let spec = Column<CustomScreen, ScreenSpec?>("spec")
    public static func `where`(_ condition: Condition) -> Query<Self> { Query<Self>().where(condition) }
    @concurrent public static func node(_ id: String, force: Bool = false, in scope: AccountScope? = nil) async throws -> Self? {
        try await readNode(relation: "node", id: id, force: force, in: scope)
    }
    @concurrent public static func create(slug: String, version: Int, requires: ScreenRequires, spec: ScreenSpec? = nil, in scope_: AccountScope? = nil) async throws -> MutationReceipt<Self> {
        var values_: [String: JSONValue] = [:]
        values_["slug"] = try JSONValue.encode(slug)
        values_["version"] = try JSONValue.encode(version)
        values_["requires"] = try JSONValue.encode(requires)
        if let value = spec { values_["spec"] = try JSONValue.encode(value) }
        return try await Self.create(values: values_, in: scope_)
    }
    @concurrent public func update(slug: String? = nil, version: Int? = nil, requires: ScreenRequires? = nil, spec: ScreenSpec?? = nil, in scope_: AccountScope? = nil) async throws -> MutationReceipt<Self> {
        var values_: [String: JSONValue] = [:]
        if let value = slug { values_["slug"] = try JSONValue.encode(value) }
        if let value = version { values_["version"] = try JSONValue.encode(value) }
        if let value = requires { values_["requires"] = try JSONValue.encode(value) }
        if let value = spec { values_["spec"] = try JSONValue.encode(value) }
        return try await self.update(values: values_, in: scope_)
    }
}

public struct StorageRow: DBModel {
    public static let modelName = "StorageRow"
    public let id: String
    public let record: String
    public let sync: SyncInfo
    public let uploads: [String: UploadInfo]
    public let rowRevision: Int64
    public let estimatedBytes: UInt64

    public init(id: String, record: String, sync: SyncInfo = SyncInfo(), uploads: [String: UploadInfo] = [:], rowRevision: Int64 = 0, estimatedBytes: UInt64 = 0) {
        self.id = id
        self.record = record
        self.sync = sync
        self.uploads = uploads
        self.rowRevision = rowRevision
        self.estimatedBytes = estimatedBytes
    }

    public static let wireLayout = "fe5fbb4f29b1efa251b84c3acf30c1c33a863db299f28340d13b798cba3d174f"

    public init(wire row: WireRow) throws {
        let r = CellReader(row.cells)
        self.id = row.id
        self.record = try r.req(0, \.asText)
        self.sync = SyncInfo(wire: row.sync)
        self.uploads = Dictionary(uniqueKeysWithValues: row.uploads.map { ($0.action, UploadInfo(wire: $0.upload)) })
        self.rowRevision = row.revision
        self.estimatedBytes = row.estimatedBytes
    }

    public static let record = Column<StorageRow, String>("record")
    public static func `where`(_ condition: Condition) -> Query<Self> { Query<Self>().where(condition) }
    @concurrent public static func create(record: String, in scope_: AccountScope? = nil) async throws -> MutationReceipt<Self> {
        var values_: [String: JSONValue] = [:]
        values_["record"] = try JSONValue.encode(record)
        return try await Self.create(values: values_, in: scope_)
    }
    @concurrent public func update(record: String? = nil, in scope_: AccountScope? = nil) async throws -> MutationReceipt<Self> {
        var values_: [String: JSONValue] = [:]
        if let value = record { values_["record"] = try JSONValue.encode(value) }
        return try await self.update(values: values_, in: scope_)
    }
}

public struct Tail: DBModel {
    public static let modelName = "Tail"
    public let id: String
    public let text: String
    public let sync: SyncInfo
    public let uploads: [String: UploadInfo]
    public let rowRevision: Int64
    public let estimatedBytes: UInt64

    public init(id: String, text: String, sync: SyncInfo = SyncInfo(), uploads: [String: UploadInfo] = [:], rowRevision: Int64 = 0, estimatedBytes: UInt64 = 0) {
        self.id = id
        self.text = text
        self.sync = sync
        self.uploads = uploads
        self.rowRevision = rowRevision
        self.estimatedBytes = estimatedBytes
    }

    public static let wireLayout = "79f6ac4394adc1b4758abd377fb226a4146dcdcfa51a32720beacc9bac4a680e"

    public init(wire row: WireRow) throws {
        let r = CellReader(row.cells)
        self.id = row.id
        self.text = try r.req(0, \.asText)
        self.sync = SyncInfo(wire: row.sync)
        self.uploads = Dictionary(uniqueKeysWithValues: row.uploads.map { ($0.action, UploadInfo(wire: $0.upload)) })
        self.rowRevision = row.revision
        self.estimatedBytes = row.estimatedBytes
    }

    public static let text = Column<Tail, String>("text")
    public static func `where`(_ condition: Condition) -> Query<Self> { Query<Self>().where(condition) }
    public static func since(_ containerId: String, afterSequence: Int, pinned: Bool) throws -> Query<Self> {
        let built_ = Query<Self>(relation: "since", params: ["containerId": try JSONValue.encode(containerId), "afterSequence": try JSONValue.encode(afterSequence), "pinned": try JSONValue.encode(pinned)])
        return built_
    }
    @concurrent public static func create(text: String, in scope_: AccountScope? = nil) async throws -> MutationReceipt<Self> {
        var values_: [String: JSONValue] = [:]
        values_["text"] = try JSONValue.encode(text)
        return try await Self.create(values: values_, in: scope_)
    }
    @concurrent public func update(text: String? = nil, in scope_: AccountScope? = nil) async throws -> MutationReceipt<Self> {
        var values_: [String: JSONValue] = [:]
        if let value = text { values_["text"] = try JSONValue.encode(value) }
        return try await self.update(values: values_, in: scope_)
    }
}

public struct UnionSample: DBModel {
    public static let modelName = "UnionSample"
    public let id: String
    public let label: String?
    public let attachment: RecordAttachment?
    public let props: MomentWidgetItemProps?
    public let sync: SyncInfo
    public let uploads: [String: UploadInfo]
    public let rowRevision: Int64
    public let estimatedBytes: UInt64

    public init(id: String, label: String? = nil, attachment: RecordAttachment? = nil, props: MomentWidgetItemProps? = nil, sync: SyncInfo = SyncInfo(), uploads: [String: UploadInfo] = [:], rowRevision: Int64 = 0, estimatedBytes: UInt64 = 0) {
        self.id = id
        self.label = label
        self.attachment = attachment
        self.props = props
        self.sync = sync
        self.uploads = uploads
        self.rowRevision = rowRevision
        self.estimatedBytes = estimatedBytes
    }

    public static let wireLayout = "423693dfea54829309e39b8ef392190ba49023893e1e8842786138a3b4a19c2a"

    public init(wire row: WireRow) throws {
        let r = CellReader(row.cells)
        self.id = row.id
        self.label = try r.opt(0, \.asText)
        self.attachment = try r.objOpt(1, RecordAttachment.init(cells:))
        self.props = try r.objOpt(2, MomentWidgetItemProps.init(cells:))
        self.sync = SyncInfo(wire: row.sync)
        self.uploads = Dictionary(uniqueKeysWithValues: row.uploads.map { ($0.action, UploadInfo(wire: $0.upload)) })
        self.rowRevision = row.revision
        self.estimatedBytes = row.estimatedBytes
    }

    public static let label = Column<UnionSample, String?>("label")
    public static let attachment = Column<UnionSample, RecordAttachmentKind>("attachment")
    public static let props = Column<UnionSample, MomentWidgetItemPropsKind>("props")
    public static func `where`(_ condition: Condition) -> Query<Self> { Query<Self>().where(condition) }
    @concurrent public static func create(label: String? = nil, attachment: RecordAttachment? = nil, props: MomentWidgetItemProps? = nil, in scope_: AccountScope? = nil) async throws -> MutationReceipt<Self> {
        var values_: [String: JSONValue] = [:]
        if let value = label { values_["label"] = try JSONValue.encode(value) }
        if let value = attachment { values_["attachment"] = try JSONValue.encode(value) }
        if let value = props { values_["props"] = try JSONValue.encode(value) }
        return try await Self.create(values: values_, in: scope_)
    }
    @concurrent public func update(label: String?? = nil, attachment: RecordAttachment?? = nil, props: MomentWidgetItemProps?? = nil, in scope_: AccountScope? = nil) async throws -> MutationReceipt<Self> {
        var values_: [String: JSONValue] = [:]
        if let value = label { values_["label"] = try JSONValue.encode(value) }
        if let value = attachment { values_["attachment"] = try JSONValue.encode(value) }
        if let value = props { values_["props"] = try JSONValue.encode(value) }
        return try await self.update(values: values_, in: scope_)
    }
}

public struct User: DBSingleton {
    public static let modelName = "User"
    public let id: String
    public let name: String
    public let avatar: FileInfo?
    public let birthday: String?
    public let typing: Bool
    public let mood: String?
    public let sync: SyncInfo
    public let uploads: [String: UploadInfo]
    public let rowRevision: Int64
    public let estimatedBytes: UInt64

    public init(id: String, name: String, avatar: FileInfo? = nil, birthday: String? = nil, typing: Bool = false, mood: String? = nil, sync: SyncInfo = SyncInfo(), uploads: [String: UploadInfo] = [:], rowRevision: Int64 = 0, estimatedBytes: UInt64 = 0) {
        self.id = id
        self.name = name
        self.avatar = avatar
        self.birthday = birthday
        self.typing = typing
        self.mood = mood
        self.sync = sync
        self.uploads = uploads
        self.rowRevision = rowRevision
        self.estimatedBytes = estimatedBytes
    }

    public static let wireLayout = "4849bf8a3175334b01f39299c6e9512f671e8acabd8119121c8cc168047fb390"

    public init(wire row: WireRow) throws {
        let r = CellReader(row.cells)
        self.id = row.id
        self.name = try r.req(0, \.asText)
        self.avatar = try r.fileOpt(1)
        self.birthday = try r.opt(2, \.asText)
        self.typing = try r.req(3, \.asBool)
        self.mood = try r.opt(4, \.asText)
        self.sync = SyncInfo(wire: row.sync)
        self.uploads = Dictionary(uniqueKeysWithValues: row.uploads.map { ($0.action, UploadInfo(wire: $0.upload)) })
        self.rowRevision = row.revision
        self.estimatedBytes = row.estimatedBytes
    }

    public enum Files: String, Sendable { case avatar }
    public static let name = Column<User, String>("name")
    public static let birthday = Column<User, String?>("birthday")
    public static func `where`(_ condition: Condition) -> Query<Self> { Query<Self>().where(condition) }
    public static var profile: Query<Self> {
        let built_ = Query<Self>(relation: "profile", params: [:])
        return built_
    }
    public static func containerMembers(_ containerId: String) throws -> Query<Self> {
        let built_ = Query<Self>(relation: "container_members", params: ["containerId": try JSONValue.encode(containerId)])
        return built_
    }
    public static func completeMembers(_ containerId: String) throws -> Query<Self> {
        let built_ = Query<Self>(relation: "complete_members", params: ["containerId": try JSONValue.encode(containerId)])
        return built_
    }
    public static var singleton: Query<Self> { Self.profile }
    @concurrent public static func create(name: String, birthday: String? = nil, in scope_: AccountScope? = nil) async throws -> MutationReceipt<Self> {
        var values_: [String: JSONValue] = [:]
        values_["name"] = try JSONValue.encode(name)
        if let value = birthday { values_["birthday"] = try JSONValue.encode(value) }
        return try await Self.create(values: values_, in: scope_)
    }
    @concurrent public func update(name: String? = nil, birthday: String?? = nil, in scope_: AccountScope? = nil) async throws -> MutationReceipt<Self> {
        var values_: [String: JSONValue] = [:]
        if let value = name { values_["name"] = try JSONValue.encode(value) }
        if let value = birthday { values_["birthday"] = try JSONValue.encode(value) }
        return try await self.update(values: values_, in: scope_)
    }
    @concurrent public func set(typing: Bool? = nil, mood: String?? = nil, in scope_: AccountScope? = nil) async throws {
        var values_: [String: JSONValue] = [:]
        if let value = typing { values_["typing"] = try JSONValue.encode(value) }
        if let value = mood { values_["mood"] = try JSONValue.encode(value) }
        try await self.set(values: values_, in: scope_)
    }
}

public struct Vibe: DBModel {
    public static let modelName = "Vibe"
    public let id: String
    public let momentId: String
    public let userId: String
    public let kind: VibeKind
    public let sync: SyncInfo
    public let uploads: [String: UploadInfo]
    public let rowRevision: Int64
    public let estimatedBytes: UInt64

    public init(id: String, momentId: String, userId: String, kind: VibeKind, sync: SyncInfo = SyncInfo(), uploads: [String: UploadInfo] = [:], rowRevision: Int64 = 0, estimatedBytes: UInt64 = 0) {
        self.id = id
        self.momentId = momentId
        self.userId = userId
        self.kind = kind
        self.sync = sync
        self.uploads = uploads
        self.rowRevision = rowRevision
        self.estimatedBytes = estimatedBytes
    }

    public static let wireLayout = "091ba73dd97a9be52544a900fa4ddd6515d3e70202d2fb1eb080d7211c9610b0"

    public init(wire row: WireRow) throws {
        let r = CellReader(row.cells)
        self.id = row.id
        self.momentId = try r.req(0, \.asId)
        self.userId = try r.req(1, \.asId)
        self.kind = try VibeKind(rawValue: r.req(2, \.asText))
        self.sync = SyncInfo(wire: row.sync)
        self.uploads = Dictionary(uniqueKeysWithValues: row.uploads.map { ($0.action, UploadInfo(wire: $0.upload)) })
        self.rowRevision = row.revision
        self.estimatedBytes = row.estimatedBytes
    }

    public static let momentId = Column<Vibe, String>("momentId")
    public static let userId = Column<Vibe, String>("userId")
    public static let kind = Column<Vibe, VibeKind>("kind")
    public static func `where`(_ condition: Condition) -> Query<Self> { Query<Self>().where(condition) }
    @concurrent public static func create(momentId: String, userId: String, kind: VibeKind, in scope_: AccountScope? = nil) async throws -> MutationReceipt<Self> {
        var values_: [String: JSONValue] = [:]
        values_["momentId"] = try JSONValue.encode(momentId)
        values_["userId"] = try JSONValue.encode(userId)
        values_["kind"] = try JSONValue.encode(kind)
        return try await Self.create(values: values_, in: scope_)
    }
    @concurrent public func update(momentId: String? = nil, userId: String? = nil, kind: VibeKind? = nil, in scope_: AccountScope? = nil) async throws -> MutationReceipt<Self> {
        var values_: [String: JSONValue] = [:]
        if let value = momentId { values_["momentId"] = try JSONValue.encode(value) }
        if let value = userId { values_["userId"] = try JSONValue.encode(value) }
        if let value = kind { values_["kind"] = try JSONValue.encode(value) }
        return try await self.update(values: values_, in: scope_)
    }
}

public struct LedgerFeedEntriesEdge: Sendable, Equatable {
    public let impressionUuid: String?
    init(_ values: [String: JSONValue]) {
        self.impressionUuid = values["impressionUuid"]?.stringValue
    }
}

public struct ShadowScopeEdge: Sendable, Equatable {
    public let score: Int?
    init(_ values: [String: JSONValue]) {
        self.score = values["score"]?.intValue
    }
}

public struct ShadowAccountEdge: Sendable, Equatable {
    public let rank: Int?
    init(_ values: [String: JSONValue]) {
        self.rank = values["rank"]?.intValue
    }
}

public struct MailboxEntriesEdge: Sendable, Equatable {
    public let score: Int?
    init(_ values: [String: JSONValue]) {
        self.score = values["score"]?.intValue
    }
}

public struct MomentFeedEdge: Sendable, Equatable {
    public let impressionUuid: String?
    public let impressionScore: Int?
    init(_ values: [String: JSONValue]) {
        self.impressionUuid = values["impressionUuid"]?.stringValue
        self.impressionScore = values["impressionScore"]?.intValue
    }
}

public struct MomentVibeFeedEdge: Sendable, Equatable {
    public let impressionUuid: String?
    public let score: Int?
    public let rank: FeedRank?
    init(_ values: [String: JSONValue]) {
        self.impressionUuid = values["impressionUuid"]?.stringValue
        self.score = values["score"]?.intValue
        self.rank = values["rank"]?.stringValue.map(FeedRank.init(rawValue:))
    }
}

public struct SampleSpec: Codable, Sendable, Equatable {
    public let root: String
    public let elements: [SampleElement]
    public let state: [SampleStateEntry]

    public init(root: String, elements: [SampleElement], state: [SampleStateEntry]) {
        self.root = root
        self.elements = elements
        self.state = state
    }

    private enum CodingKeys: String, CodingKey {
        case root = "root"
        case elements = "elements"
        case state = "state"
    }

    public init(cells: [Cell]) throws {
        let r = CellReader(cells)
        self.root = try r.req(0, \.asId)
        self.elements = try r.arr(1, SampleElement.init(cells:))
        self.state = try r.arr(2, SampleStateEntry.init(cells:))
    }
}

public enum SampleElement: Codable, Sendable, Equatable {
    case button(SampleButton)
    case column(SampleColumn)
    case heading(SampleHeading)
    case pressable(SamplePressable)
    case spacer(SampleSpacer)
    case `switch`(SampleSwitch)
    case unknown(typename: String)

    public init(from decoder: Decoder) throws {
        let typename = try decoder.container(keyedBy: UnionTypenameKey.self).decode(String.self, forKey: .typename)
        switch typename {
        case "SampleButton": self = .button(try SampleButton(from: decoder))
        case "SampleColumn": self = .column(try SampleColumn(from: decoder))
        case "SampleHeading": self = .heading(try SampleHeading(from: decoder))
        case "SamplePressable": self = .pressable(try SamplePressable(from: decoder))
        case "SampleSpacer": self = .spacer(try SampleSpacer(from: decoder))
        case "SampleSwitch": self = .`switch`(try SampleSwitch(from: decoder))
        default: self = .unknown(typename: typename)
        }
    }

    public func encode(to encoder: Encoder) throws {
        switch self {
        case let .button(value): try JSONValue.tagged("SampleButton", value).encode(to: encoder)
        case let .column(value): try JSONValue.tagged("SampleColumn", value).encode(to: encoder)
        case let .heading(value): try JSONValue.tagged("SampleHeading", value).encode(to: encoder)
        case let .pressable(value): try JSONValue.tagged("SamplePressable", value).encode(to: encoder)
        case let .spacer(value): try JSONValue.tagged("SampleSpacer", value).encode(to: encoder)
        case let .`switch`(value): try JSONValue.tagged("SampleSwitch", value).encode(to: encoder)
        case let .unknown(typename): try JSONValue.tagged(typename, [String: String]()).encode(to: encoder)
        }
    }

    public init(cells: [Cell]) throws {
        let r = CellReader(cells)
        let typename = try r.req(0, \.asText)
        switch typename {
        case "SampleButton": self = .button(try r.obj(1, SampleButton.init(cells:)))
        case "SampleColumn": self = .column(try r.obj(1, SampleColumn.init(cells:)))
        case "SampleHeading": self = .heading(try r.obj(1, SampleHeading.init(cells:)))
        case "SamplePressable": self = .pressable(try r.obj(1, SamplePressable.init(cells:)))
        case "SampleSpacer": self = .spacer(try r.obj(1, SampleSpacer.init(cells:)))
        case "SampleSwitch": self = .`switch`(try r.obj(1, SampleSwitch.init(cells:)))
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

    public var press: SampleAction? {
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

    public var visible: SampleVisibility? {
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

    public var kind: SampleElementKind? {
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

public enum SampleElementKind: String, Codable, Sendable {
    case button = "SampleButton"
    case column = "SampleColumn"
    case heading = "SampleHeading"
    case pressable = "SamplePressable"
    case spacer = "SampleSpacer"
    case `switch` = "SampleSwitch"
}

public struct SampleButton: Codable, Sendable, Equatable {
    public let id: String
    public let children: [String]
    public let press: SampleAction?
    public let visible: SampleVisibility?
    public let label: SampleTextValue?
    public let disabled: SampleFlagValue?

    public init(id: String, children: [String], press: SampleAction? = nil, visible: SampleVisibility? = nil, label: SampleTextValue? = nil, disabled: SampleFlagValue? = nil) {
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

    public init(cells: [Cell]) throws {
        let r = CellReader(cells)
        self.id = try r.req(0, \.asId)
        self.children = try r.list(1, \.asId)
        self.press = try r.objOpt(2, SampleAction.init(cells:))
        self.visible = try r.objOpt(3, SampleVisibility.init(cells:))
        self.label = try r.objOpt(4, SampleTextValue.init(cells:))
        self.disabled = try r.objOpt(5, SampleFlagValue.init(cells:))
    }
}

public enum SampleAction: Codable, Sendable, Equatable {
    case navigate(SampleNavigateAction)
    case openRecord(SampleOpenRecordAction)
    case openExternal(SampleOpenExternalAction)
    case openProfile(SampleOpenProfileAction)
    case unknown(typename: String)

    public init(from decoder: Decoder) throws {
        let typename = try decoder.container(keyedBy: UnionTypenameKey.self).decode(String.self, forKey: .typename)
        switch typename {
        case "SampleNavigateAction": self = .navigate(try SampleNavigateAction(from: decoder))
        case "SampleOpenRecordAction": self = .openRecord(try SampleOpenRecordAction(from: decoder))
        case "SampleOpenExternalAction": self = .openExternal(try SampleOpenExternalAction(from: decoder))
        case "SampleOpenProfileAction": self = .openProfile(try SampleOpenProfileAction(from: decoder))
        default: self = .unknown(typename: typename)
        }
    }

    public func encode(to encoder: Encoder) throws {
        switch self {
        case let .navigate(value): try JSONValue.tagged("SampleNavigateAction", value).encode(to: encoder)
        case let .openRecord(value): try JSONValue.tagged("SampleOpenRecordAction", value).encode(to: encoder)
        case let .openExternal(value): try JSONValue.tagged("SampleOpenExternalAction", value).encode(to: encoder)
        case let .openProfile(value): try JSONValue.tagged("SampleOpenProfileAction", value).encode(to: encoder)
        case let .unknown(typename): try JSONValue.tagged(typename, [String: String]()).encode(to: encoder)
        }
    }

    public init(cells: [Cell]) throws {
        let r = CellReader(cells)
        let typename = try r.req(0, \.asText)
        switch typename {
        case "SampleNavigateAction": self = .navigate(try r.obj(1, SampleNavigateAction.init(cells:)))
        case "SampleOpenRecordAction": self = .openRecord(try r.obj(1, SampleOpenRecordAction.init(cells:)))
        case "SampleOpenExternalAction": self = .openExternal(try r.obj(1, SampleOpenExternalAction.init(cells:)))
        case "SampleOpenProfileAction": self = .openProfile(try r.obj(1, SampleOpenProfileAction.init(cells:)))
        default: self = .unknown(typename: typename)
        }
    }

    public var kind: SampleActionKind? {
        switch self {
        case .navigate: return .navigate
        case .openRecord: return .openRecord
        case .openExternal: return .openExternal
        case .openProfile: return .openProfile
        case .unknown: return nil
        }
    }
}

public enum SampleActionKind: String, Codable, Sendable {
    case navigate = "SampleNavigateAction"
    case openRecord = "SampleOpenRecordAction"
    case openExternal = "SampleOpenExternalAction"
    case openProfile = "SampleOpenProfileAction"
}

public struct SampleNavigateAction: Codable, Sendable, Equatable {
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

    public init(cells: [Cell]) throws {
        let r = CellReader(cells)
        self.slug = try r.req(0, \.asText)
        self.title = try r.opt(1, \.asText)
    }
}

public struct SampleOpenRecordAction: Codable, Sendable, Equatable {
    public let recordId: String

    public init(recordId: String) {
        self.recordId = recordId
    }

    private enum CodingKeys: String, CodingKey {
        case recordId = "recordId"
    }

    public init(cells: [Cell]) throws {
        let r = CellReader(cells)
        self.recordId = try r.req(0, \.asText)
    }
}

public struct SampleOpenExternalAction: Codable, Sendable, Equatable {
    public let url: String

    public init(url: String) {
        self.url = url
    }

    private enum CodingKeys: String, CodingKey {
        case url = "url"
    }

    public init(cells: [Cell]) throws {
        let r = CellReader(cells)
        self.url = try r.req(0, \.asText)
    }
}

public struct SampleOpenProfileAction: Codable, Sendable, Equatable {
    public let userId: String

    public init(userId: String) {
        self.userId = userId
    }

    private enum CodingKeys: String, CodingKey {
        case userId = "userId"
    }

    public init(cells: [Cell]) throws {
        let r = CellReader(cells)
        self.userId = try r.req(0, \.asText)
    }
}

public enum SampleVisibility: Codable, Sendable, Equatable {
    case condition(SampleCondition)
    case visibleAll(SampleVisibleAll)
    case visibleAny(SampleVisibleAny)
    case visibleFlag(SampleVisibleFlag)
    case unknown(typename: String)

    public init(from decoder: Decoder) throws {
        let typename = try decoder.container(keyedBy: UnionTypenameKey.self).decode(String.self, forKey: .typename)
        switch typename {
        case "SampleCondition": self = .condition(try SampleCondition(from: decoder))
        case "SampleVisibleAll": self = .visibleAll(try SampleVisibleAll(from: decoder))
        case "SampleVisibleAny": self = .visibleAny(try SampleVisibleAny(from: decoder))
        case "SampleVisibleFlag": self = .visibleFlag(try SampleVisibleFlag(from: decoder))
        default: self = .unknown(typename: typename)
        }
    }

    public func encode(to encoder: Encoder) throws {
        switch self {
        case let .condition(value): try JSONValue.tagged("SampleCondition", value).encode(to: encoder)
        case let .visibleAll(value): try JSONValue.tagged("SampleVisibleAll", value).encode(to: encoder)
        case let .visibleAny(value): try JSONValue.tagged("SampleVisibleAny", value).encode(to: encoder)
        case let .visibleFlag(value): try JSONValue.tagged("SampleVisibleFlag", value).encode(to: encoder)
        case let .unknown(typename): try JSONValue.tagged(typename, [String: String]()).encode(to: encoder)
        }
    }

    public init(cells: [Cell]) throws {
        let r = CellReader(cells)
        let typename = try r.req(0, \.asText)
        switch typename {
        case "SampleCondition": self = .condition(try r.obj(1, SampleCondition.init(cells:)))
        case "SampleVisibleAll": self = .visibleAll(try r.obj(1, SampleVisibleAll.init(cells:)))
        case "SampleVisibleAny": self = .visibleAny(try r.obj(1, SampleVisibleAny.init(cells:)))
        case "SampleVisibleFlag": self = .visibleFlag(try r.obj(1, SampleVisibleFlag.init(cells:)))
        default: self = .unknown(typename: typename)
        }
    }

    public var kind: SampleVisibilityKind? {
        switch self {
        case .condition: return .condition
        case .visibleAll: return .visibleAll
        case .visibleAny: return .visibleAny
        case .visibleFlag: return .visibleFlag
        case .unknown: return nil
        }
    }
}

public enum SampleVisibilityKind: String, Codable, Sendable {
    case condition = "SampleCondition"
    case visibleAll = "SampleVisibleAll"
    case visibleAny = "SampleVisibleAny"
    case visibleFlag = "SampleVisibleFlag"
}

public struct SampleCondition: Codable, Sendable, Equatable {
    public let comparison: String
    public let negated: Bool
    public let operand: SampleOperand?
    public let path: String

    public init(comparison: String, negated: Bool, operand: SampleOperand? = nil, path: String) {
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

    public init(cells: [Cell]) throws {
        let r = CellReader(cells)
        self.comparison = try r.req(0, \.asText)
        self.negated = try r.req(1, \.asBool)
        self.operand = try r.objOpt(2, SampleOperand.init(cells:))
        self.path = try r.req(3, \.asText)
    }
}

public enum SampleOperand: Codable, Sendable, Equatable {
    case flagLiteral(SampleFlagLiteral)
    case numberLiteral(SampleNumberLiteral)
    case stateRef(SampleStateRef)
    case textLiteral(SampleTextLiteral)
    case unknown(typename: String)

    public init(from decoder: Decoder) throws {
        let typename = try decoder.container(keyedBy: UnionTypenameKey.self).decode(String.self, forKey: .typename)
        switch typename {
        case "SampleFlagLiteral": self = .flagLiteral(try SampleFlagLiteral(from: decoder))
        case "SampleNumberLiteral": self = .numberLiteral(try SampleNumberLiteral(from: decoder))
        case "SampleStateRef": self = .stateRef(try SampleStateRef(from: decoder))
        case "SampleTextLiteral": self = .textLiteral(try SampleTextLiteral(from: decoder))
        default: self = .unknown(typename: typename)
        }
    }

    public func encode(to encoder: Encoder) throws {
        switch self {
        case let .flagLiteral(value): try JSONValue.tagged("SampleFlagLiteral", value).encode(to: encoder)
        case let .numberLiteral(value): try JSONValue.tagged("SampleNumberLiteral", value).encode(to: encoder)
        case let .stateRef(value): try JSONValue.tagged("SampleStateRef", value).encode(to: encoder)
        case let .textLiteral(value): try JSONValue.tagged("SampleTextLiteral", value).encode(to: encoder)
        case let .unknown(typename): try JSONValue.tagged(typename, [String: String]()).encode(to: encoder)
        }
    }

    public init(cells: [Cell]) throws {
        let r = CellReader(cells)
        let typename = try r.req(0, \.asText)
        switch typename {
        case "SampleFlagLiteral": self = .flagLiteral(try r.obj(1, SampleFlagLiteral.init(cells:)))
        case "SampleNumberLiteral": self = .numberLiteral(try r.obj(1, SampleNumberLiteral.init(cells:)))
        case "SampleStateRef": self = .stateRef(try r.obj(1, SampleStateRef.init(cells:)))
        case "SampleTextLiteral": self = .textLiteral(try r.obj(1, SampleTextLiteral.init(cells:)))
        default: self = .unknown(typename: typename)
        }
    }

    public var kind: SampleOperandKind? {
        switch self {
        case .flagLiteral: return .flagLiteral
        case .numberLiteral: return .numberLiteral
        case .stateRef: return .stateRef
        case .textLiteral: return .textLiteral
        case .unknown: return nil
        }
    }
}

public enum SampleOperandKind: String, Codable, Sendable {
    case flagLiteral = "SampleFlagLiteral"
    case numberLiteral = "SampleNumberLiteral"
    case stateRef = "SampleStateRef"
    case textLiteral = "SampleTextLiteral"
}

public struct SampleFlagLiteral: Codable, Sendable, Equatable {
    public let flag: Bool

    public init(flag: Bool) {
        self.flag = flag
    }

    private enum CodingKeys: String, CodingKey {
        case flag = "flag"
    }

    public init(cells: [Cell]) throws {
        let r = CellReader(cells)
        self.flag = try r.req(0, \.asBool)
    }
}

public struct SampleNumberLiteral: Codable, Sendable, Equatable {
    public let number: Double

    public init(number: Double) {
        self.number = number
    }

    private enum CodingKeys: String, CodingKey {
        case number = "number"
    }

    public init(cells: [Cell]) throws {
        let r = CellReader(cells)
        self.number = try r.req(0, \.asDouble)
    }
}

public struct SampleStateRef: Codable, Sendable, Equatable {
    public let path: String

    public init(path: String) {
        self.path = path
    }

    private enum CodingKeys: String, CodingKey {
        case path = "path"
    }

    public init(cells: [Cell]) throws {
        let r = CellReader(cells)
        self.path = try r.req(0, \.asText)
    }
}

public struct SampleTextLiteral: Codable, Sendable, Equatable {
    public let text: String

    public init(text: String) {
        self.text = text
    }

    private enum CodingKeys: String, CodingKey {
        case text = "text"
    }

    public init(cells: [Cell]) throws {
        let r = CellReader(cells)
        self.text = try r.req(0, \.asText)
    }
}

public struct SampleVisibleAll: Codable, Sendable, Equatable {
    public let conditions: [SampleCondition]

    public init(conditions: [SampleCondition]) {
        self.conditions = conditions
    }

    private enum CodingKeys: String, CodingKey {
        case conditions = "conditions"
    }

    public init(cells: [Cell]) throws {
        let r = CellReader(cells)
        self.conditions = try r.arr(0, SampleCondition.init(cells:))
    }
}

public struct SampleVisibleAny: Codable, Sendable, Equatable {
    public let conditions: [SampleCondition]

    public init(conditions: [SampleCondition]) {
        self.conditions = conditions
    }

    private enum CodingKeys: String, CodingKey {
        case conditions = "conditions"
    }

    public init(cells: [Cell]) throws {
        let r = CellReader(cells)
        self.conditions = try r.arr(0, SampleCondition.init(cells:))
    }
}

public struct SampleVisibleFlag: Codable, Sendable, Equatable {
    public let flag: Bool

    public init(flag: Bool) {
        self.flag = flag
    }

    private enum CodingKeys: String, CodingKey {
        case flag = "flag"
    }

    public init(cells: [Cell]) throws {
        let r = CellReader(cells)
        self.flag = try r.req(0, \.asBool)
    }
}

public enum SampleTextValue: Codable, Sendable, Equatable {
    case stateRef(SampleStateRef)
    case textLiteral(SampleTextLiteral)
    case unknown(typename: String)

    public init(from decoder: Decoder) throws {
        let typename = try decoder.container(keyedBy: UnionTypenameKey.self).decode(String.self, forKey: .typename)
        switch typename {
        case "SampleStateRef": self = .stateRef(try SampleStateRef(from: decoder))
        case "SampleTextLiteral": self = .textLiteral(try SampleTextLiteral(from: decoder))
        default: self = .unknown(typename: typename)
        }
    }

    public func encode(to encoder: Encoder) throws {
        switch self {
        case let .stateRef(value): try JSONValue.tagged("SampleStateRef", value).encode(to: encoder)
        case let .textLiteral(value): try JSONValue.tagged("SampleTextLiteral", value).encode(to: encoder)
        case let .unknown(typename): try JSONValue.tagged(typename, [String: String]()).encode(to: encoder)
        }
    }

    public init(cells: [Cell]) throws {
        let r = CellReader(cells)
        let typename = try r.req(0, \.asText)
        switch typename {
        case "SampleStateRef": self = .stateRef(try r.obj(1, SampleStateRef.init(cells:)))
        case "SampleTextLiteral": self = .textLiteral(try r.obj(1, SampleTextLiteral.init(cells:)))
        default: self = .unknown(typename: typename)
        }
    }

    public var kind: SampleTextValueKind? {
        switch self {
        case .stateRef: return .stateRef
        case .textLiteral: return .textLiteral
        case .unknown: return nil
        }
    }
}

public enum SampleTextValueKind: String, Codable, Sendable {
    case stateRef = "SampleStateRef"
    case textLiteral = "SampleTextLiteral"
}

public enum SampleFlagValue: Codable, Sendable, Equatable {
    case flagLiteral(SampleFlagLiteral)
    case stateRef(SampleStateRef)
    case unknown(typename: String)

    public init(from decoder: Decoder) throws {
        let typename = try decoder.container(keyedBy: UnionTypenameKey.self).decode(String.self, forKey: .typename)
        switch typename {
        case "SampleFlagLiteral": self = .flagLiteral(try SampleFlagLiteral(from: decoder))
        case "SampleStateRef": self = .stateRef(try SampleStateRef(from: decoder))
        default: self = .unknown(typename: typename)
        }
    }

    public func encode(to encoder: Encoder) throws {
        switch self {
        case let .flagLiteral(value): try JSONValue.tagged("SampleFlagLiteral", value).encode(to: encoder)
        case let .stateRef(value): try JSONValue.tagged("SampleStateRef", value).encode(to: encoder)
        case let .unknown(typename): try JSONValue.tagged(typename, [String: String]()).encode(to: encoder)
        }
    }

    public init(cells: [Cell]) throws {
        let r = CellReader(cells)
        let typename = try r.req(0, \.asText)
        switch typename {
        case "SampleFlagLiteral": self = .flagLiteral(try r.obj(1, SampleFlagLiteral.init(cells:)))
        case "SampleStateRef": self = .stateRef(try r.obj(1, SampleStateRef.init(cells:)))
        default: self = .unknown(typename: typename)
        }
    }

    public var kind: SampleFlagValueKind? {
        switch self {
        case .flagLiteral: return .flagLiteral
        case .stateRef: return .stateRef
        case .unknown: return nil
        }
    }
}

public enum SampleFlagValueKind: String, Codable, Sendable {
    case flagLiteral = "SampleFlagLiteral"
    case stateRef = "SampleStateRef"
}

public struct SampleColumn: Codable, Sendable, Equatable {
    public let id: String
    public let children: [String]
    public let press: SampleAction?
    public let visible: SampleVisibility?
    public let gap: SampleNumberValue?
    public let alignItems: String?

    public init(id: String, children: [String], press: SampleAction? = nil, visible: SampleVisibility? = nil, gap: SampleNumberValue? = nil, alignItems: String? = nil) {
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

    public init(cells: [Cell]) throws {
        let r = CellReader(cells)
        self.id = try r.req(0, \.asId)
        self.children = try r.list(1, \.asId)
        self.press = try r.objOpt(2, SampleAction.init(cells:))
        self.visible = try r.objOpt(3, SampleVisibility.init(cells:))
        self.gap = try r.objOpt(4, SampleNumberValue.init(cells:))
        self.alignItems = try r.opt(5, \.asText)
    }
}

public enum SampleNumberValue: Codable, Sendable, Equatable {
    case numberLiteral(SampleNumberLiteral)
    case stateRef(SampleStateRef)
    case unknown(typename: String)

    public init(from decoder: Decoder) throws {
        let typename = try decoder.container(keyedBy: UnionTypenameKey.self).decode(String.self, forKey: .typename)
        switch typename {
        case "SampleNumberLiteral": self = .numberLiteral(try SampleNumberLiteral(from: decoder))
        case "SampleStateRef": self = .stateRef(try SampleStateRef(from: decoder))
        default: self = .unknown(typename: typename)
        }
    }

    public func encode(to encoder: Encoder) throws {
        switch self {
        case let .numberLiteral(value): try JSONValue.tagged("SampleNumberLiteral", value).encode(to: encoder)
        case let .stateRef(value): try JSONValue.tagged("SampleStateRef", value).encode(to: encoder)
        case let .unknown(typename): try JSONValue.tagged(typename, [String: String]()).encode(to: encoder)
        }
    }

    public init(cells: [Cell]) throws {
        let r = CellReader(cells)
        let typename = try r.req(0, \.asText)
        switch typename {
        case "SampleNumberLiteral": self = .numberLiteral(try r.obj(1, SampleNumberLiteral.init(cells:)))
        case "SampleStateRef": self = .stateRef(try r.obj(1, SampleStateRef.init(cells:)))
        default: self = .unknown(typename: typename)
        }
    }

    public var kind: SampleNumberValueKind? {
        switch self {
        case .numberLiteral: return .numberLiteral
        case .stateRef: return .stateRef
        case .unknown: return nil
        }
    }
}

public enum SampleNumberValueKind: String, Codable, Sendable {
    case numberLiteral = "SampleNumberLiteral"
    case stateRef = "SampleStateRef"
}

public struct SampleHeading: Codable, Sendable, Equatable {
    public let id: String
    public let children: [String]
    public let press: SampleAction?
    public let visible: SampleVisibility?
    public let text: SampleTextValue?
    public let level: Int?

    public init(id: String, children: [String], press: SampleAction? = nil, visible: SampleVisibility? = nil, text: SampleTextValue? = nil, level: Int? = nil) {
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

    public init(cells: [Cell]) throws {
        let r = CellReader(cells)
        self.id = try r.req(0, \.asId)
        self.children = try r.list(1, \.asId)
        self.press = try r.objOpt(2, SampleAction.init(cells:))
        self.visible = try r.objOpt(3, SampleVisibility.init(cells:))
        self.text = try r.objOpt(4, SampleTextValue.init(cells:))
        self.level = try r.opt(5, \.asInt)
    }
}

public struct SamplePressable: Codable, Sendable, Equatable {
    public let id: String
    public let children: [String]
    public let press: SampleAction?
    public let visible: SampleVisibility?

    public init(id: String, children: [String], press: SampleAction? = nil, visible: SampleVisibility? = nil) {
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

    public init(cells: [Cell]) throws {
        let r = CellReader(cells)
        self.id = try r.req(0, \.asId)
        self.children = try r.list(1, \.asId)
        self.press = try r.objOpt(2, SampleAction.init(cells:))
        self.visible = try r.objOpt(3, SampleVisibility.init(cells:))
    }
}

public struct SampleSpacer: Codable, Sendable, Equatable {
    public let id: String
    public let children: [String]
    public let press: SampleAction?
    public let visible: SampleVisibility?
    public let length: SampleNumberValue?

    public init(id: String, children: [String], press: SampleAction? = nil, visible: SampleVisibility? = nil, length: SampleNumberValue? = nil) {
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

    public init(cells: [Cell]) throws {
        let r = CellReader(cells)
        self.id = try r.req(0, \.asId)
        self.children = try r.list(1, \.asId)
        self.press = try r.objOpt(2, SampleAction.init(cells:))
        self.visible = try r.objOpt(3, SampleVisibility.init(cells:))
        self.length = try r.objOpt(4, SampleNumberValue.init(cells:))
    }
}

public struct SampleSwitch: Codable, Sendable, Equatable {
    public let id: String
    public let children: [String]
    public let press: SampleAction?
    public let visible: SampleVisibility?
    public let checked: SampleFlagValue?
    public let label: SampleTextValue?

    public init(id: String, children: [String], press: SampleAction? = nil, visible: SampleVisibility? = nil, checked: SampleFlagValue? = nil, label: SampleTextValue? = nil) {
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

    public init(cells: [Cell]) throws {
        let r = CellReader(cells)
        self.id = try r.req(0, \.asId)
        self.children = try r.list(1, \.asId)
        self.press = try r.objOpt(2, SampleAction.init(cells:))
        self.visible = try r.objOpt(3, SampleVisibility.init(cells:))
        self.checked = try r.objOpt(4, SampleFlagValue.init(cells:))
        self.label = try r.objOpt(5, SampleTextValue.init(cells:))
    }
}

public struct SampleStateEntry: Codable, Sendable, Equatable {
    public let path: String
    public let value: SampleStateValue

    public init(path: String, value: SampleStateValue) {
        self.path = path
        self.value = value
    }

    private enum CodingKeys: String, CodingKey {
        case path = "path"
        case value = "value"
    }

    public init(cells: [Cell]) throws {
        let r = CellReader(cells)
        self.path = try r.req(0, \.asText)
        self.value = try r.obj(1, SampleStateValue.init(cells:))
    }
}

public enum SampleStateValue: Codable, Sendable, Equatable {
    case flag(SampleFlagLiteral)
    case number(SampleNumberLiteral)
    case text(SampleTextLiteral)
    case unknown(typename: String)

    public init(from decoder: Decoder) throws {
        let typename = try decoder.container(keyedBy: UnionTypenameKey.self).decode(String.self, forKey: .typename)
        switch typename {
        case "SampleFlagLiteral": self = .flag(try SampleFlagLiteral(from: decoder))
        case "SampleNumberLiteral": self = .number(try SampleNumberLiteral(from: decoder))
        case "SampleTextLiteral": self = .text(try SampleTextLiteral(from: decoder))
        default: self = .unknown(typename: typename)
        }
    }

    public func encode(to encoder: Encoder) throws {
        switch self {
        case let .flag(value): try JSONValue.tagged("SampleFlagLiteral", value).encode(to: encoder)
        case let .number(value): try JSONValue.tagged("SampleNumberLiteral", value).encode(to: encoder)
        case let .text(value): try JSONValue.tagged("SampleTextLiteral", value).encode(to: encoder)
        case let .unknown(typename): try JSONValue.tagged(typename, [String: String]()).encode(to: encoder)
        }
    }

    public init(cells: [Cell]) throws {
        let r = CellReader(cells)
        let typename = try r.req(0, \.asText)
        switch typename {
        case "SampleFlagLiteral": self = .flag(try r.obj(1, SampleFlagLiteral.init(cells:)))
        case "SampleNumberLiteral": self = .number(try r.obj(1, SampleNumberLiteral.init(cells:)))
        case "SampleTextLiteral": self = .text(try r.obj(1, SampleTextLiteral.init(cells:)))
        default: self = .unknown(typename: typename)
        }
    }

    public var kind: SampleStateValueKind? {
        switch self {
        case .flag: return .flag
        case .number: return .number
        case .text: return .text
        case .unknown: return nil
        }
    }
}

public enum SampleStateValueKind: String, Codable, Sendable {
    case flag = "SampleFlagLiteral"
    case number = "SampleNumberLiteral"
    case text = "SampleTextLiteral"
}

public struct KeywordValue: Codable, Sendable, Equatable {
    public let `default`: String
    public let when: String
    public let `class`: String
    public let container: String
    public let r: String
    public let child: KeywordChild

    public init(`default`: String, when: String, `class`: String, container: String, r: String, child: KeywordChild = KeywordChild(default: "d", when: "w", class: "c")) {
        self.`default` = `default`
        self.when = when
        self.`class` = `class`
        self.container = container
        self.r = r
        self.child = child
    }

    private enum CodingKeys: String, CodingKey {
        case `default` = "default"
        case when = "when"
        case `class` = "class"
        case container = "container"
        case r = "r"
        case child = "child"
    }

    public init(cells: [Cell]) throws {
        let r = CellReader(cells)
        self.`default` = try r.req(0, \.asText)
        self.when = try r.req(1, \.asText)
        self.`class` = try r.req(2, \.asText)
        self.container = try r.req(3, \.asText)
        self.r = try r.req(4, \.asText)
        self.child = try r.obj(5, KeywordChild.init(cells:))
    }
}

public struct KeywordChild: Codable, Sendable, Equatable {
    public let `default`: String
    public let when: String
    public let `class`: String

    public init(`default`: String, when: String, `class`: String) {
        self.`default` = `default`
        self.when = when
        self.`class` = `class`
    }

    private enum CodingKeys: String, CodingKey {
        case `default` = "default"
        case when = "when"
        case `class` = "class"
    }

    public init(cells: [Cell]) throws {
        let r = CellReader(cells)
        self.`default` = try r.req(0, \.asText)
        self.when = try r.req(1, \.asText)
        self.`class` = try r.req(2, \.asText)
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

    public init(title: String, note: String? = nil, nullableDefault: String? = "default", ownerId: String? = nil, count: Int = 2, ratio: Double = 1.5, enabled: Bool = false, createdAt: String? = nil, state: NestedValueState = NestedValueState.ready, labels: [String] = [], counts: [Int] = [], ratios: [Double] = [], flags: [Bool] = [], dates: [String] = [], owners: [String] = [], payload: JSONValue? = nil, rawValue: JSONValue = .object(["free": .array([.integer(1), .bool(true), .null])]), preferences: NestedValuePreferences = NestedValuePreferences(label: "base"), presets: [NestedValuePreferences] = [NestedValuePreferences(label: "first")], metadata: JSONValue? = nil, values: JSONValue? = nil, child: NestedValueChild? = nil, children: [NestedValueChild] = []) {
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

    public init(cells: [Cell]) throws {
        let r = CellReader(cells)
        self.title = try r.req(0, \.asText)
        self.note = try r.opt(1, \.asText)
        self.nullableDefault = try r.opt(2, \.asText)
        self.ownerId = try r.opt(3, \.asId)
        self.count = try r.req(4, \.asInt)
        self.ratio = try r.req(5, \.asDouble)
        self.enabled = try r.req(6, \.asBool)
        self.createdAt = try r.opt(7, \.asText)
        self.state = try NestedValueState(rawValue: r.req(8, \.asText))
        self.labels = try r.list(9, \.asText)
        self.counts = try r.list(10, \.asInt)
        self.ratios = try r.list(11, \.asDouble)
        self.flags = try r.list(12, \.asBool)
        self.dates = try r.list(13, \.asText)
        self.owners = try r.list(14, \.asId)
        self.payload = try r.jsonOpt(15)
        self.rawValue = try r.json(16)
        self.preferences = try r.obj(17, NestedValuePreferences.init(cells:))
        self.presets = try r.arr(18, NestedValuePreferences.init(cells:))
        self.metadata = try r.jsonOpt(19)
        self.values = try r.jsonOpt(20)
        self.child = try r.objOpt(21, NestedValueChild.init(cells:))
        self.children = try r.arr(22, NestedValueChild.init(cells:))
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

    public init(cells: [Cell]) throws {
        let r = CellReader(cells)
        self.label = try r.req(0, \.asText)
        self.active = try r.req(1, \.asBool)
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

    public init(cells: [Cell]) throws {
        let r = CellReader(cells)
        self.body = try r.opt(0, \.asText)
        self.horizontal = try r.req(1, \.asDouble)
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

    public init(cells: [Cell]) throws {
        let r = CellReader(cells)
        self.name = try r.req(0, \.asText)
        self.children = try r.arr(1, NestedTree.init(cells:))
    }
}

public struct RecordReplyNode: Codable, Sendable, Equatable {
    public let id: String?
    public let body: String?
    public let kind: String?
    public let userId: String?
    public let user: RecordUserRef?
    public let media: NestedMedia?

    public init(id: String? = nil, body: String? = nil, kind: String? = nil, userId: String? = nil, user: RecordUserRef? = nil, media: NestedMedia? = nil) {
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

    public init(cells: [Cell]) throws {
        let r = CellReader(cells)
        self.id = try r.opt(0, \.asId)
        self.body = try r.opt(1, \.asText)
        self.kind = try r.opt(2, \.asText)
        self.userId = try r.opt(3, \.asId)
        self.user = try r.objOpt(4, RecordUserRef.init(cells:))
        self.media = try r.objOpt(5, NestedMedia.init(cells:))
    }
}

public struct RecordUserRef: Codable, Sendable, Equatable {
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

    public init(cells: [Cell]) throws {
        let r = CellReader(cells)
        self.id = try r.opt(0, \.asId)
        self.fullName = try r.opt(1, \.asText)
        self.name = try r.opt(2, \.asText)
        self.username = try r.opt(3, \.asText)
        self.avatarUrl = try r.opt(4, \.asText)
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

    public init(cells: [Cell]) throws {
        let r = CellReader(cells)
        self.id = try r.req(0, \.asId)
        self.fileUrl = try r.opt(1, \.asText)
    }
}

public struct RecordReactionNode: Codable, Sendable, Equatable {
    public let body: String?
    public let userId: String?
    public let user: RecordReactionUser?

    public init(body: String? = nil, userId: String? = nil, user: RecordReactionUser? = nil) {
        self.body = body
        self.userId = userId
        self.user = user
    }

    private enum CodingKeys: String, CodingKey {
        case body = "body"
        case userId = "user.id"
        case user = "user"
    }

    public init(cells: [Cell]) throws {
        let r = CellReader(cells)
        self.body = try r.opt(0, \.asText)
        self.userId = try r.opt(1, \.asId)
        self.user = try r.objOpt(2, RecordReactionUser.init(cells:))
    }
}

public struct RecordReactionUser: Codable, Sendable, Equatable {
    public let id: String?
    public let name: String?

    public init(id: String? = nil, name: String? = nil) {
        self.id = id
        self.name = name
    }

    private enum CodingKeys: String, CodingKey {
        case id = "id"
        case name = "name"
    }

    public init(cells: [Cell]) throws {
        let r = CellReader(cells)
        self.id = try r.opt(0, \.asId)
        self.name = try r.opt(1, \.asText)
    }
}

public enum RecordAttachment: Codable, Sendable, Equatable {
    case reward(RewardAttachment)
    case moment(MomentAttachment)
    case profile(ProfileAttachment)
    case reaction(ReactionAttachment)
    case unknown(typename: String)

    public init(from decoder: Decoder) throws {
        let typename = try decoder.container(keyedBy: UnionTypenameKey.self).decode(String.self, forKey: .typename)
        switch typename {
        case "RewardAttachment": self = .reward(try RewardAttachment(from: decoder))
        case "MomentAttachment": self = .moment(try MomentAttachment(from: decoder))
        case "ProfileAttachment": self = .profile(try ProfileAttachment(from: decoder))
        case "ReactionAttachment": self = .reaction(try ReactionAttachment(from: decoder))
        default: self = .unknown(typename: typename)
        }
    }

    public func encode(to encoder: Encoder) throws {
        switch self {
        case let .reward(value): try JSONValue.tagged("RewardAttachment", value).encode(to: encoder)
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
        case "RewardAttachment": self = .reward(try r.obj(1, RewardAttachment.init(cells:)))
        case "MomentAttachment": self = .moment(try r.obj(1, MomentAttachment.init(cells:)))
        case "ProfileAttachment": self = .profile(try r.obj(1, ProfileAttachment.init(cells:)))
        case "ReactionAttachment": self = .reaction(try r.obj(1, ReactionAttachment.init(cells:)))
        default: self = .unknown(typename: typename)
        }
    }

    public var kind: RecordAttachmentKind? {
        switch self {
        case .reward: return .reward
        case .moment: return .moment
        case .profile: return .profile
        case .reaction: return .reaction
        case .unknown: return nil
        }
    }
}

public enum RecordAttachmentKind: String, Codable, Sendable {
    case reward = "RewardAttachment"
    case moment = "MomentAttachment"
    case profile = "ProfileAttachment"
    case reaction = "ReactionAttachment"
}

public struct RewardAttachment: Codable, Sendable, Equatable {
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
        case tier = "rewardTier"
        case status = "status"
        case tags = "tags"
    }

    public init(cells: [Cell]) throws {
        let r = CellReader(cells)
        self.id = try r.req(0, \.asId)
        self.tier = try r.req(1, \.asText)
        self.status = try r.opt(2, \.asText)
        self.tags = try r.list(3, \.asText)
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

    public init(cells: [Cell]) throws {
        let r = CellReader(cells)
        self.momentId = try r.req(0, \.asId)
        self.caption = try r.opt(1, \.asText)
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

    public init(cells: [Cell]) throws {
        let r = CellReader(cells)
        self.user = try r.objOpt(0, AttachmentUser.init(cells:))
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

    public init(cells: [Cell]) throws {
        let r = CellReader(cells)
        self.id = try r.req(0, \.asId)
        self.name = try r.opt(1, \.asText)
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

    public init(cells: [Cell]) throws {
        let r = CellReader(cells)
        self.body = try r.req(0, \.asText)
    }
}

public struct RecordMedia: Codable, Sendable, Equatable {
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

    public init(cells: [Cell]) throws {
        let r = CellReader(cells)
        self.kind = try r.opt(0, \.asText)
        self.transcoded = try r.opt(1, \.asBool)
        self.fileUrl = try r.opt(2, \.asText)
    }
}

public struct ScreenRequires: Codable, Sendable, Equatable {
    public let ios: Int?
    public let android: Int?

    public init(ios: Int? = nil, android: Int? = nil) {
        self.ios = ios
        self.android = android
    }

    private enum CodingKeys: String, CodingKey {
        case ios = "ios"
        case android = "android"
    }

    public init(cells: [Cell]) throws {
        let r = CellReader(cells)
        self.ios = try r.opt(0, \.asInt)
        self.android = try r.opt(1, \.asInt)
    }
}

public struct ScreenSpec: Codable, Sendable, Equatable {
    public let root: String
    public let state: [ScreenStateEntry]
    public let elements: [ScreenElement]

    public init(root: String, state: [ScreenStateEntry], elements: [ScreenElement]) {
        self.root = root
        self.state = state
        self.elements = elements
    }

    private enum CodingKeys: String, CodingKey {
        case root = "root"
        case state = "state"
        case elements = "elements"
    }

    public init(cells: [Cell]) throws {
        let r = CellReader(cells)
        self.root = try r.req(0, \.asId)
        self.state = try r.arr(1, ScreenStateEntry.init(cells:))
        self.elements = try r.arr(2, ScreenElement.init(cells:))
    }
}

public struct ScreenStateEntry: Codable, Sendable, Equatable {
    public let path: String
    public let value: JSONValue

    public init(path: String, value: JSONValue) {
        self.path = path
        self.value = value
    }

    private enum CodingKeys: String, CodingKey {
        case path = "path"
        case value = "value"
    }

    public init(cells: [Cell]) throws {
        let r = CellReader(cells)
        self.path = try r.req(0, \.asText)
        self.value = try r.json(1)
    }
}

public struct ScreenElement: Codable, Sendable, Equatable {
    public let id: String
    public let type: String
    public let slot: String?
    public let children: [String]
    public let props: JSONValue?
    public let visible: JSONValue?
    public let press: JSONValue?
    public let change: JSONValue?
    public let padding: String?
    public let margin: String?
    public let background: String?
    public let radius: String?
    public let expand: Bool?
    public let align: String?
    public let accessibilityLabel: String?

    public init(id: String, type: String, slot: String? = nil, children: [String], props: JSONValue? = nil, visible: JSONValue? = nil, press: JSONValue? = nil, change: JSONValue? = nil, padding: String? = nil, margin: String? = nil, background: String? = nil, radius: String? = nil, expand: Bool? = nil, align: String? = nil, accessibilityLabel: String? = nil) {
        self.id = id
        self.type = type
        self.slot = slot
        self.children = children
        self.props = props
        self.visible = visible
        self.press = press
        self.change = change
        self.padding = padding
        self.margin = margin
        self.background = background
        self.radius = radius
        self.expand = expand
        self.align = align
        self.accessibilityLabel = accessibilityLabel
    }

    private enum CodingKeys: String, CodingKey {
        case id = "id"
        case type = "type"
        case slot = "slot"
        case children = "children"
        case props = "props"
        case visible = "visible"
        case press = "press"
        case change = "change"
        case padding = "padding"
        case margin = "margin"
        case background = "background"
        case radius = "radius"
        case expand = "expand"
        case align = "align"
        case accessibilityLabel = "accessibilityLabel"
    }

    public init(cells: [Cell]) throws {
        let r = CellReader(cells)
        self.id = try r.req(0, \.asId)
        self.type = try r.req(1, \.asText)
        self.slot = try r.opt(2, \.asText)
        self.children = try r.list(3, \.asId)
        self.props = try r.jsonOpt(4)
        self.visible = try r.jsonOpt(5)
        self.press = try r.jsonOpt(6)
        self.change = try r.jsonOpt(7)
        self.padding = try r.opt(8, \.asText)
        self.margin = try r.opt(9, \.asText)
        self.background = try r.opt(10, \.asText)
        self.radius = try r.opt(11, \.asText)
        self.expand = try r.opt(12, \.asBool)
        self.align = try r.opt(13, \.asText)
        self.accessibilityLabel = try r.opt(14, \.asText)
    }
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

    public init(cells: [Cell]) throws {
        let r = CellReader(cells)
        self.emoji = try r.req(0, \.asText)
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

    public init(cells: [Cell]) throws {
        let r = CellReader(cells)
        self.text = try r.req(0, \.asText)
        self.size = try r.opt(1, \.asInt)
    }
}

public enum ContainerShelf: Codable, Sendable, Hashable {
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

public enum MediaMessageKind: Codable, Sendable, Hashable {
    case text
    case gift
    case unknown(String)

    public init(rawValue: String) {
        switch rawValue {
        case "TEXT": self = .text
        case "GIFT": self = .gift
        default: self = .unknown(rawValue)
        }
    }

    public var rawValue: String {
        switch self {
        case .text: return "TEXT"
        case .gift: return "GIFT"
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

public enum MediaKindValue: Codable, Sendable, Hashable {
    case audio
    case image
    case video
    case unknown(String)

    public init(rawValue: String) {
        switch rawValue {
        case "AUDIO": self = .audio
        case "IMAGE": self = .image
        case "VIDEO": self = .video
        default: self = .unknown(rawValue)
        }
    }

    public var rawValue: String {
        switch self {
        case .audio: return "AUDIO"
        case .image: return "IMAGE"
        case .video: return "VIDEO"
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

public enum FeedRank: Codable, Sendable, Hashable {
    case top
    case normal
    case unknown(String)

    public init(rawValue: String) {
        switch rawValue {
        case "top": self = .top
        case "normal": self = .normal
        default: self = .unknown(rawValue)
        }
    }

    public var rawValue: String {
        switch self {
        case .top: return "top"
        case .normal: return "normal"
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

public let modelsJSON = #"{"models":[{"key":"Ledger","fields":[{"name":"id","kind":{"type":"scalar","kind":"id"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"scope","kind":{"type":"scalar","kind":"str"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null}],"associations":[],"relations":[],"indexes":[["scope"]],"actions":[{"client_mutation_id":false,"name":"create","root_shape":"upsert","on_failure":"rollback","has_optimistic":true,"has_temp_row_retention":true,"has_cascade_dependents":false,"members":[],"mutation":"CreateMoment","upload":false,"variables":{},"optimistic":{},"refetch":[],"args":{},"local":[],"correlate":[],"ignore_response":false,"wrap":null,"root":null,"lands":[],"result":{}},{"client_mutation_id":false,"name":"update","root_shape":"upsert","on_failure":"rollback","has_optimistic":false,"has_temp_row_retention":false,"has_cascade_dependents":false,"members":[],"mutation":"UpdateMoment","upload":false,"variables":{},"optimistic":{},"refetch":[],"args":{},"local":[],"correlate":[],"ignore_response":false,"wrap":null,"root":null,"lands":[],"result":{}},{"client_mutation_id":false,"name":"move","root_shape":"upsert","on_failure":"rollback","has_optimistic":false,"has_temp_row_retention":false,"has_cascade_dependents":false,"members":[],"mutation":"LikeMoment","upload":false,"variables":{"scope":"scope"},"optimistic":{},"refetch":[],"args":{"scope":"string"},"local":[],"correlate":[],"ignore_response":false,"wrap":null,"root":null,"lands":[],"result":{}}],"ephemeral":[],"storage":"disk","expire_after":null,"listens":[]},{"key":"LedgerFeed","fields":[{"name":"id","kind":{"type":"scalar","kind":"id"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"title","kind":{"type":"scalar","kind":"str"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null}],"associations":[],"relations":[{"name":"entries","by_columns":[],"text_params":["scope"],"remote":{"operation":"MomentsFeed","shape":{"type":"connection","direction":"forward","coverage":"page","max_pages":null,"cursor":null},"stale_time":"5.minutes","empty_stale_time":null,"selects_page_info":true,"page_size":20,"variables":{},"root":null},"is_global":false,"sort":"server_order","joins_events":false,"joins_created":false,"edge_fields":[{"name":"impressionUuid","kind":{"type":"scalar","kind":"str"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null}]}],"indexes":[],"actions":[],"ephemeral":[],"storage":"disk","expire_after":null,"listens":[]},{"key":"Shadow","fields":[{"name":"id","kind":{"type":"scalar","kind":"id"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"title","kind":{"type":"scalar","kind":"str"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null}],"associations":[],"relations":[{"name":"scope","by_columns":[],"text_params":[],"remote":{"operation":"MomentsFeed","shape":{"type":"connection","direction":"forward","coverage":"page","max_pages":null,"cursor":null},"stale_time":"5.minutes","empty_stale_time":null,"selects_page_info":true,"page_size":20,"variables":{},"root":null},"is_global":true,"sort":"server_order","joins_events":false,"joins_created":false,"edge_fields":[{"name":"score","kind":{"type":"scalar","kind":"int"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null}]},{"name":"account","by_columns":[],"text_params":[],"remote":{"operation":"MomentsFeed","shape":{"type":"connection","direction":"forward","coverage":"page","max_pages":null,"cursor":null},"stale_time":"5.minutes","empty_stale_time":null,"selects_page_info":true,"page_size":20,"variables":{},"root":null},"is_global":true,"sort":"server_order","joins_events":false,"joins_created":false,"edge_fields":[{"name":"rank","kind":{"type":"scalar","kind":"int"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null}]}],"indexes":[],"actions":[{"client_mutation_id":false,"name":"create","root_shape":"upsert","on_failure":"rollback","has_optimistic":true,"has_temp_row_retention":true,"has_cascade_dependents":false,"members":[],"mutation":"CreateMoment","upload":false,"variables":{},"optimistic":{},"refetch":[],"args":{},"local":[],"correlate":[],"ignore_response":false,"wrap":null,"root":null,"lands":[],"result":{}}],"ephemeral":[],"storage":"disk","expire_after":null,"listens":[]},{"key":"Holder","fields":[{"name":"id","kind":{"type":"scalar","kind":"id"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"label","kind":{"type":"scalar","kind":"str"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null}],"associations":[],"relations":[{"name":"account","by_columns":[],"text_params":[],"remote":{"operation":"AccountProfile","shape":{"type":"single"},"stale_time":"5.minutes","empty_stale_time":null,"selects_page_info":false,"page_size":null,"variables":{},"root":null},"is_global":true,"sort":"server_order","joins_events":false,"joins_created":false}],"indexes":[],"actions":[],"ephemeral":[],"storage":"disk","expire_after":null,"listens":[]},{"key":"Mailbox","fields":[{"name":"id","kind":{"type":"scalar","kind":"id"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"read","kind":{"type":"scalar","kind":"bool"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"put","kind":{"type":"scalar","kind":"str"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null}],"associations":[],"relations":[{"name":"entries","by_columns":[],"text_params":["act"],"remote":{"operation":"MomentsFeed","shape":{"type":"connection","direction":"forward","coverage":"page","max_pages":null,"cursor":null},"stale_time":"5.minutes","empty_stale_time":null,"selects_page_info":true,"page_size":20,"variables":{},"root":null},"is_global":false,"sort":"server_order","joins_events":false,"joins_created":false,"edge_fields":[{"name":"score","kind":{"type":"scalar","kind":"int"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null}]}],"indexes":[],"actions":[{"client_mutation_id":false,"name":"mark","root_shape":"upsert","on_failure":"rollback","has_optimistic":false,"has_temp_row_retention":false,"has_cascade_dependents":false,"members":[],"mutation":"LikeMoment","upload":false,"variables":{"put":"put"},"optimistic":{},"refetch":[],"args":{"put":"string"},"local":[],"correlate":[],"ignore_response":false,"wrap":null,"root":null,"lands":[],"result":{}}],"ephemeral":[],"storage":"disk","expire_after":null,"listens":[]},{"key":"Asset","fields":[{"name":"id","kind":{"type":"scalar","kind":"id"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"clientKey","kind":{"type":"scalar","kind":"str"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"size","kind":{"type":"scalar","kind":"int"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"position","kind":{"type":"scalar","kind":"int"},"nullable":false,"optional":false,"default":0,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null}],"associations":[],"relations":[],"indexes":[],"actions":[{"client_mutation_id":false,"name":"create_file","root_shape":"upsert","on_failure":"rollback","has_optimistic":false,"has_temp_row_retention":false,"has_cascade_dependents":false,"members":[],"mutation":"CreateAsset","upload":true,"variables":{"assetId":"id"},"optimistic":{},"refetch":[],"args":{},"local":[],"correlate":["clientKey"],"ignore_response":false,"wrap":null,"root":"createAsset.asset","lands":[],"result":{}},{"batch":{"min":2,"max":10,"items":"items","single":"create_file","input":{"clientKey":"clientKey"},"renumber":{"position":"position","size":"size","position_from":0}},"client_mutation_id":false,"name":"create_files","root_shape":"upsert","on_failure":"rollback","has_optimistic":false,"has_temp_row_retention":false,"has_cascade_dependents":false,"members":[],"mutation":"CreateAssets","upload":true,"variables":{"assetId":"id"},"optimistic":{},"refetch":[],"args":{},"local":[],"correlate":["clientKey"],"ignore_response":false,"wrap":null,"root":"createAssets.assets","lands":[],"result":{}}],"ephemeral":[],"storage":"disk","expire_after":null,"listens":[],"upload_steps":{"create_blob":{"name":"CreateBlob","root_path":"blobCreate"},"multipart":{"initiate":{"name":"InitMultipart"},"presign":{"name":"PresignParts"},"complete":{"name":"CompleteMultipart"},"abort":{"name":"AbortMultipart"}}}},{"key":"Club","fields":[{"name":"id","kind":{"type":"scalar","kind":"id"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"name","kind":{"type":"scalar","kind":"str"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null}],"associations":[],"relations":[],"indexes":[],"actions":[{"client_mutation_id":false,"name":"create","root_shape":"upsert","on_failure":"rollback","has_optimistic":true,"has_temp_row_retention":true,"has_cascade_dependents":false,"members":[],"mutation":"CreateClub","upload":false,"variables":{"name":"name"},"optimistic":{},"refetch":[],"args":{},"local":[],"correlate":[],"ignore_response":false,"wrap":null,"root":null,"lands":[],"result":{}},{"client_mutation_id":false,"name":"leave","root_shape":"upsert","on_failure":"rollback","has_optimistic":false,"has_temp_row_retention":false,"has_cascade_dependents":false,"members":[],"mutation":"LeaveClub","upload":false,"variables":{"clubId":"id"},"optimistic":{},"refetch":[],"args":{},"local":[],"correlate":[],"ignore_response":true,"wrap":null,"root":null,"lands":[],"result":{},"removes_receiver":true},{"client_mutation_id":false,"name":"add_member","root_shape":"upsert","on_failure":"rollback","has_optimistic":false,"has_temp_row_retention":false,"has_cascade_dependents":false,"members":[],"mutation":"AddClubMember","upload":false,"variables":{"clubId":"id","userIds":"userIds"},"optimistic":{},"refetch":["Person.club_members"],"args":{"userIds":"ids"},"local":[],"correlate":[],"ignore_response":false,"wrap":null,"root":"addClubMember","lands":[],"result":{},"scope_effects":[{"kind":"join","model":"Person","scope":"club_members","rows":"userIds","with":{"clubId":"id"},"order_param":"order","orders":["JOINED","OWNER_FIRST"]}]},{"client_mutation_id":false,"name":"remove_member","root_shape":"upsert","on_failure":"rollback","has_optimistic":false,"has_temp_row_retention":false,"has_cascade_dependents":false,"members":[],"mutation":"RemoveClubMember","upload":false,"variables":{"clubId":"id","userIds":"userIds"},"optimistic":{},"refetch":["Person.club_members"],"args":{"userIds":"ids"},"local":[],"correlate":[],"ignore_response":false,"wrap":null,"root":"removeClubMember","lands":[],"result":{},"scope_effects":[{"kind":"leave","model":"Person","scope":"club_members","rows":"userIds","with":{"clubId":"id"},"order_param":"order","orders":["JOINED","OWNER_FIRST"]}]}],"ephemeral":[],"storage":"disk","expire_after":null,"listens":[]},{"key":"Person","fields":[{"name":"id","kind":{"type":"scalar","kind":"id"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"name","kind":{"type":"scalar","kind":"str"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null}],"associations":[],"relations":[{"name":"club_members","by_columns":[],"text_params":["clubId","order"],"remote":{"operation":"ClubMembers","shape":{"type":"connection","direction":"forward","coverage":"page","max_pages":null,"cursor":null},"stale_time":"5.minutes","empty_stale_time":null,"selects_page_info":true,"page_size":30,"variables":{},"root":"node.members"},"is_global":false,"sort":"server_order","joins_events":false,"joins_created":false}],"indexes":[],"actions":[],"ephemeral":[],"storage":"disk","expire_after":null,"listens":[]},{"key":"Letter","fields":[{"name":"id","kind":{"type":"scalar","kind":"id"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"chatId","kind":{"type":"scalar","kind":"id"},"nullable":false,"optional":false,"default":null,"indexed":true,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"text","kind":{"type":"scalar","kind":"str"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"at","kind":{"type":"scalar","kind":"int"},"nullable":false,"optional":false,"default":null,"indexed":true,"id_from_parent":null,"from":null,"app_type":null,"merge":null}],"associations":[],"relations":[{"name":"thread","by_columns":["chatId"],"text_params":[],"remote":null,"is_global":false,"sort":{"fields":[{"column":"at","direction":"asc"}]},"joins_events":false,"joins_created":false}],"indexes":[],"actions":[{"client_mutation_id":false,"name":"create","root_shape":"upsert","on_failure":"rollback","has_optimistic":true,"has_temp_row_retention":true,"has_cascade_dependents":false,"members":[],"mutation":"SendLetter","upload":false,"variables":{"at":"at","chatId":"chatId","text":"text"},"optimistic":{},"refetch":[],"args":{},"local":[],"correlate":[],"ignore_response":false,"wrap":null,"root":null,"lands":[],"result":{}},{"client_mutation_id":false,"name":"forward","root_shape":"upsert","on_failure":"rollback","has_optimistic":false,"has_temp_row_retention":false,"has_cascade_dependents":false,"members":[],"mutation":"ForwardLetter","upload":false,"variables":{"chatId":"targetChatId","letterId":"id"},"optimistic":{},"refetch":[],"args":{"targetChatId":"id"},"local":[],"correlate":[],"ignore_response":false,"wrap":null,"root":"forwardLetter.letter","lands":[],"result":{},"copies_receiver_to":{"arg":"targetChatId","field":"chatId"}}],"ephemeral":[],"storage":"disk","expire_after":null,"listens":[]},{"key":"Container","fields":[{"name":"id","kind":{"type":"scalar","kind":"id"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"title","kind":{"type":"scalar","kind":"str"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"lastRecordAt","kind":{"type":"scalar","kind":"date"},"nullable":true,"optional":false,"default":null,"indexed":true,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"lastRecordId","kind":{"type":"scalar","kind":"id"},"nullable":true,"optional":false,"default":null,"indexed":true,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"shareUrl","kind":{"type":"scalar","kind":"str"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"isPublic","kind":{"type":"scalar","kind":"bool"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"shelves","kind":{"type":"list","element":"str"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"ContainerShelf","values":["PRIMARY","SECONDARY"],"merge":null}],"associations":[{"name":"records","kind":"has_many","target":"Record","foreign_key":"containerId","dependent":"destroy","touches_parent":false,"sets":{},"newer":[],"from":null},{"name":"lastRecord","kind":"belongs_to","target":"Record","foreign_key":"lastRecordId","dependent":null,"touches_parent":false,"sets":{},"newer":[],"from":null}],"relations":[{"name":"node","by_columns":[],"text_params":["nodeId"],"remote":{"operation":"ContainerNode","shape":{"type":"single"},"stale_time":"5.minutes","empty_stale_time":null,"selects_page_info":false,"page_size":null,"variables":{"id":{"from":"param","name":"nodeId"}},"root":null},"is_global":false,"sort":"server_order","joins_events":false,"joins_created":false,"node":"one"},{"name":"nodes","by_columns":[],"text_params":[],"remote":{"operation":"ContainersByIds","shape":{"type":"list","coverage":"complete"},"stale_time":"5.minutes","empty_stale_time":null,"selects_page_info":false,"page_size":null,"variables":{},"root":null},"is_global":true,"sort":"server_order","joins_events":false,"joins_created":false,"node":"many"},{"name":"recent","by_columns":[],"text_params":[],"remote":null,"is_global":true,"sort":{"fields":[{"column":"lastRecordAt","direction":"desc"}]},"joins_events":false,"joins_created":false},{"name":"shelf","membership":{"shelf":{"contains":"shelves"}},"joins_node":true,"by_columns":[],"text_params":["shelf"],"text_types":{"shelf":"ContainerShelf"},"remote":{"operation":"ContainerShelfQuery","shape":{"type":"list","coverage":"complete"},"stale_time":"5.minutes","empty_stale_time":null,"selects_page_info":false,"page_size":null,"variables":{},"root":"containers"},"is_global":false,"sort":"server_order","joins_events":true,"joins_created":false}],"indexes":[],"actions":[{"client_mutation_id":false,"name":"create","root_shape":"upsert","on_failure":"rollback","has_optimistic":true,"has_temp_row_retention":true,"has_cascade_dependents":false,"members":[],"mutation":"CreateContainer","upload":false,"variables":{},"optimistic":{},"refetch":[],"args":{},"local":[],"correlate":[],"ignore_response":false,"wrap":null,"root":null,"lands":[],"result":{}},{"client_mutation_id":false,"name":"create_photo","root_shape":"upsert","on_failure":"rollback","has_optimistic":false,"has_temp_row_retention":false,"has_cascade_dependents":false,"members":[],"mutation":"ContainerGroupCreateFile","upload":true,"variables":{"containerId":"id"},"optimistic":{},"refetch":[],"args":{},"local":[],"correlate":[],"ignore_response":false,"wrap":null,"root":"groupContainerCreate.container","lands":[],"result":{}},{"client_mutation_id":false,"name":"update_photo","root_shape":"upsert","on_failure":"rollback","has_optimistic":false,"has_temp_row_retention":false,"has_cascade_dependents":false,"members":[],"mutation":"ContainerGroupUpdateFile","upload":true,"variables":{"containerId":"id"},"optimistic":{},"refetch":[],"args":{},"local":[],"correlate":[],"ignore_response":false,"wrap":null,"root":"groupContainerUpdate.container","lands":[],"result":{}}],"ephemeral":[],"storage":"disk","expire_after":null,"listens":[],"changes":[{"area":"CONTAINER","relation":"node","parameter":null,"removes_on":["DELETED","ACCESS_REVOKED"]}],"change_feed":{"subscription":"RecordEvents","root":"recordEvents.entries","area":"topic","kind":"action","id":"recordId"},"upload_steps":{"create_blob":{"name":"CreateBlob","root_path":"blobCreate"},"multipart":{"initiate":{"name":"InitMultipart"},"presign":{"name":"PresignParts"},"complete":{"name":"CompleteMultipart"},"abort":{"name":"AbortMultipart"}}}},{"key":"Document","fields":[{"name":"id","kind":{"type":"scalar","kind":"id"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"slug","kind":{"type":"scalar","kind":"str"},"nullable":false,"optional":false,"default":null,"indexed":true,"id_from_parent":null,"from":null,"app_type":null,"merge":null}],"associations":[],"relations":[{"name":"bySlug","updates":[{"subscription":"documentUpdated","deleted_by":null,"set":{},"removes":false,"debounce":null,"root":null,"apply":{"effect":"upsert","matches":{}}}],"by_columns":["slug"],"text_params":["source"],"remote":{"operation":"documentBySlug","shape":{"type":"list","coverage":"complete"},"stale_time":"5.minutes","empty_stale_time":null,"selects_page_info":false,"page_size":null,"variables":{},"root":"documentBySlug"},"is_global":false,"sort":"server_order","joins_events":false,"joins_created":false}],"indexes":[],"actions":[],"ephemeral":[],"storage":"disk","expire_after":null,"listens":[]},{"key":"Chat","fields":[{"name":"id","kind":{"type":"scalar","kind":"id"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"title","kind":{"type":"scalar","kind":"str"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"serverRevision","kind":{"type":"scalar","kind":"int"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":"revision","app_type":null,"merge":null},{"name":"lists","kind":{"type":"list","element":"str"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null}],"associations":[],"relations":[{"name":"node","by_columns":[],"text_params":["nodeId"],"remote":{"operation":"ChatNode","shape":{"type":"single"},"stale_time":"5.minutes","empty_stale_time":null,"selects_page_info":false,"page_size":null,"variables":{"id":{"from":"param","name":"nodeId"}},"root":null},"is_global":false,"sort":"server_order","joins_events":false,"joins_created":false,"node":"one"},{"name":"list","membership":{"list":{"contains":"lists"}},"joins_node":true,"by_columns":[],"text_params":["list"],"remote":{"operation":"ChatList","shape":{"type":"list","coverage":"complete"},"stale_time":"5.minutes","empty_stale_time":null,"selects_page_info":false,"page_size":null,"variables":{},"root":"chats"},"is_global":false,"sort":"server_order","joins_events":true,"joins_created":false}],"indexes":[],"actions":[],"ephemeral":[],"storage":"disk","expire_after":null,"listens":[],"changes":[{"area":"MESSAGE","relation":"node","parameter":null,"payload":"chat"},{"area":"CHAT","relation":"node","parameter":null,"removes_on":["DELETED","ACCESS_REVOKED"]}],"server_revision":"serverRevision"},{"key":"Message","fields":[{"name":"id","kind":{"type":"scalar","kind":"id"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"text","kind":{"type":"scalar","kind":"str"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"chatId","kind":{"type":"scalar","kind":"id"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null}],"associations":[],"relations":[{"name":"node","by_columns":[],"text_params":["nodeId"],"remote":{"operation":"MessageNode","shape":{"type":"single"},"stale_time":"5.minutes","empty_stale_time":null,"selects_page_info":false,"page_size":null,"variables":{"id":{"from":"param","name":"nodeId"}},"root":null},"is_global":false,"sort":"server_order","joins_events":false,"joins_created":false,"node":"one"}],"indexes":[],"actions":[],"ephemeral":[],"storage":"disk","expire_after":null,"listens":[],"changes":[{"area":"MESSAGE","relation":"node","parameter":null,"removes_on":["DELETED","ACCESS_REVOKED"],"payload":"message"}]},{"key":"InterfaceRecord","fields":[{"name":"id","kind":{"type":"scalar","kind":"id"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"slug","kind":{"type":"scalar","kind":"str"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"spec","kind":{"type":"object"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"SampleSpec","merge":null,"fields":[{"name":"root","kind":{"type":"scalar","kind":"id"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"elements","kind":{"type":"union_array"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"SampleElement","merge":null,"fields":[{"name":"button","kind":{"type":"object"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"SampleButton","merge":null,"fields":[{"name":"label","kind":{"type":"union"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"SampleTextValue","merge":null,"fields":[{"name":"stateRef","kind":{"type":"object"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"SampleStateRef","merge":null},{"name":"textLiteral","kind":{"type":"object"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"SampleTextLiteral","merge":null}]},{"name":"disabled","kind":{"type":"union"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"SampleFlagValue","merge":null,"fields":[{"name":"flagLiteral","kind":{"type":"object"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"SampleFlagLiteral","merge":null},{"name":"stateRef","kind":{"type":"object"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"SampleStateRef","merge":null}]}]},{"name":"column","kind":{"type":"object"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"SampleColumn","merge":null,"fields":[{"name":"gap","kind":{"type":"union"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"SampleNumberValue","merge":null,"fields":[{"name":"numberLiteral","kind":{"type":"object"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"SampleNumberLiteral","merge":null},{"name":"stateRef","kind":{"type":"object"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"SampleStateRef","merge":null}]},{"name":"alignItems","kind":{"type":"scalar","kind":"str"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null}]},{"name":"heading","kind":{"type":"object"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"SampleHeading","merge":null,"fields":[{"name":"text","kind":{"type":"union"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"SampleTextValue","merge":null},{"name":"level","kind":{"type":"scalar","kind":"int"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null}]},{"name":"pressable","kind":{"type":"object"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"SamplePressable","merge":null},{"name":"spacer","kind":{"type":"object"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"SampleSpacer","merge":null,"fields":[{"name":"length","kind":{"type":"union"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"SampleNumberValue","merge":null}]},{"name":"switch","kind":{"type":"object"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"SampleSwitch","merge":null,"fields":[{"name":"checked","kind":{"type":"union"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"SampleFlagValue","merge":null},{"name":"label","kind":{"type":"union"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"SampleTextValue","merge":null}]}],"shared":[{"name":"id","kind":{"type":"scalar","kind":"id"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"children","kind":{"type":"list","element":"id"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"press","kind":{"type":"union"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"SampleAction","merge":null,"fields":[{"name":"navigate","kind":{"type":"object"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"SampleNavigateAction","merge":null,"fields":[{"name":"slug","kind":{"type":"scalar","kind":"str"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"title","kind":{"type":"scalar","kind":"str"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null}]},{"name":"openRecord","kind":{"type":"object"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"SampleOpenRecordAction","merge":null,"fields":[{"name":"recordId","kind":{"type":"scalar","kind":"str"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null}]},{"name":"openExternal","kind":{"type":"object"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"SampleOpenExternalAction","merge":null,"fields":[{"name":"url","kind":{"type":"scalar","kind":"str"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null}]},{"name":"openProfile","kind":{"type":"object"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"SampleOpenProfileAction","merge":null,"fields":[{"name":"userId","kind":{"type":"scalar","kind":"str"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null}]}]},{"name":"visible","kind":{"type":"union"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"SampleVisibility","merge":null,"fields":[{"name":"condition","kind":{"type":"object"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"SampleCondition","merge":null,"fields":[{"name":"comparison","kind":{"type":"scalar","kind":"str"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"negated","kind":{"type":"scalar","kind":"bool"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"operand","kind":{"type":"union"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"SampleOperand","merge":null,"fields":[{"name":"flagLiteral","kind":{"type":"object"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"SampleFlagLiteral","merge":null,"fields":[{"name":"flag","kind":{"type":"scalar","kind":"bool"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null}]},{"name":"numberLiteral","kind":{"type":"object"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"SampleNumberLiteral","merge":null,"fields":[{"name":"number","kind":{"type":"scalar","kind":"num"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null}]},{"name":"stateRef","kind":{"type":"object"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"SampleStateRef","merge":null,"fields":[{"name":"path","kind":{"type":"scalar","kind":"str"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null}]},{"name":"textLiteral","kind":{"type":"object"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"SampleTextLiteral","merge":null,"fields":[{"name":"text","kind":{"type":"scalar","kind":"str"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null}]}]},{"name":"path","kind":{"type":"scalar","kind":"str"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null}]},{"name":"visibleAll","kind":{"type":"object"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"SampleVisibleAll","merge":null,"fields":[{"name":"conditions","kind":{"type":"array"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"SampleCondition","merge":null}]},{"name":"visibleAny","kind":{"type":"object"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"SampleVisibleAny","merge":null,"fields":[{"name":"conditions","kind":{"type":"array"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"SampleCondition","merge":null}]},{"name":"visibleFlag","kind":{"type":"object"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"SampleVisibleFlag","merge":null,"fields":[{"name":"flag","kind":{"type":"scalar","kind":"bool"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null}]}]}]},{"name":"state","kind":{"type":"array"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"SampleStateEntry","merge":null,"fields":[{"name":"path","kind":{"type":"scalar","kind":"str"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"value","kind":{"type":"union"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"SampleStateValue","merge":null,"fields":[{"name":"flag","kind":{"type":"object"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"SampleFlagLiteral","merge":null},{"name":"number","kind":{"type":"object"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"SampleNumberLiteral","merge":null},{"name":"text","kind":{"type":"object"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"SampleTextLiteral","merge":null}]}]}]}],"associations":[],"relations":[{"name":"node","by_columns":[],"text_params":["nodeId"],"remote":{"operation":"SampleNode","shape":{"type":"single"},"stale_time":"5.minutes","empty_stale_time":null,"selects_page_info":false,"page_size":null,"variables":{"id":{"from":"param","name":"nodeId"}},"root":null},"is_global":false,"sort":"server_order","joins_events":false,"joins_created":false,"node":"one"}],"indexes":[],"actions":[],"ephemeral":[],"storage":"disk","expire_after":null,"listens":[]},{"key":"KeywordRecord","fields":[{"name":"id","kind":{"type":"scalar","kind":"id"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"default","kind":{"type":"scalar","kind":"str"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"class","kind":{"type":"scalar","kind":"str"},"nullable":false,"optional":false,"default":null,"indexed":true,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"value","kind":{"type":"object"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"KeywordValue","merge":null,"fields":[{"name":"default","kind":{"type":"scalar","kind":"str"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"when","kind":{"type":"scalar","kind":"str"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"class","kind":{"type":"scalar","kind":"str"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"container","kind":{"type":"scalar","kind":"str"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"r","kind":{"type":"scalar","kind":"str"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"child","kind":{"type":"object"},"nullable":false,"optional":false,"default":{"class":"c","default":"d","when":"w"},"indexed":false,"id_from_parent":null,"from":null,"app_type":"KeywordChild","merge":null,"fields":[{"name":"default","kind":{"type":"scalar","kind":"str"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"when","kind":{"type":"scalar","kind":"str"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"class","kind":{"type":"scalar","kind":"str"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null}]}]}],"associations":[],"relations":[{"name":"when","by_columns":["class"],"text_params":[],"remote":null,"is_global":false,"sort":"server_order","joins_events":false,"joins_created":false}],"indexes":[],"actions":[],"ephemeral":[],"storage":"local","expire_after":null,"listens":[]},{"key":"MediaMessage","fields":[{"name":"id","kind":{"type":"scalar","kind":"id"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"chatId","kind":{"type":"scalar","kind":"id"},"nullable":false,"optional":false,"default":null,"indexed":true,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"kind","kind":{"type":"enum","values":["TEXT","GIFT"]},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"MediaMessageKind","merge":null},{"name":"mediaKind","kind":{"type":"enum","values":["AUDIO","IMAGE","VIDEO"]},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"MediaKindValue","merge":null},{"name":"sequenceNumber","kind":{"type":"scalar","kind":"int"},"nullable":true,"optional":false,"default":null,"indexed":true,"id_from_parent":null,"from":null,"app_type":null,"merge":null}],"associations":[],"relations":[{"name":"visual_media","row_rules":[{"field":"mediaKind","test":"in","values":["IMAGE","VIDEO"]}],"joins_node":true,"by_columns":["chatId"],"text_params":[],"remote":{"operation":"ChatMedia","shape":{"type":"connection","direction":"forward","coverage":"page","max_pages":null,"cursor":null},"stale_time":"5.minutes","empty_stale_time":null,"selects_page_info":true,"page_size":30,"variables":{"includeGifts":{"from":"constant","value":false},"mediaKinds":{"from":"constant","value":["IMAGE","VIDEO"]}},"root":"node.messages"},"is_global":false,"sort":{"fields":[{"column":"sequenceNumber","direction":"asc"}]},"joins_events":true,"joins_created":false},{"name":"audio_media","row_rules":[{"field":"mediaKind","test":"equals","value":"AUDIO"}],"joins_node":true,"by_columns":["chatId"],"text_params":[],"remote":{"operation":"ChatMedia","shape":{"type":"connection","direction":"forward","coverage":"page","max_pages":null,"cursor":null},"stale_time":"5.minutes","empty_stale_time":null,"selects_page_info":true,"page_size":30,"variables":{"includeGifts":{"from":"constant","value":false},"mediaKinds":{"from":"constant","value":["AUDIO"]}},"root":"node.messages"},"is_global":false,"sort":{"fields":[{"column":"sequenceNumber","direction":"asc"}]},"joins_events":true,"joins_created":false},{"name":"gift_media","row_rules":[{"field":"kind","test":"equals","value":"GIFT"}],"joins_node":true,"by_columns":["chatId"],"text_params":[],"remote":{"operation":"ChatMedia","shape":{"type":"connection","direction":"forward","coverage":"page","max_pages":null,"cursor":null},"stale_time":"5.minutes","empty_stale_time":null,"selects_page_info":true,"page_size":30,"variables":{"includeGifts":{"from":"constant","value":true}},"root":"node.messages"},"is_global":false,"sort":{"fields":[{"column":"sequenceNumber","direction":"asc"}]},"joins_events":true,"joins_created":false},{"name":"visual_since","row_rules":[{"field":"mediaKind","test":"in","values":["IMAGE","VIDEO"]},{"field":"sequenceNumber","test":"greater_than","parameter":"afterSequence"}],"joins_node":true,"by_columns":["chatId"],"text_params":["afterSequence"],"scalar_types":{"afterSequence":"int"},"remote":{"operation":"ChatMedia","shape":{"type":"connection","direction":"forward","coverage":"page","max_pages":null,"cursor":null},"stale_time":"5.minutes","empty_stale_time":null,"selects_page_info":true,"page_size":50,"variables":{"includeGifts":{"from":"constant","value":false},"mediaKinds":{"from":"constant","value":["IMAGE","VIDEO"]}},"root":"node.messages"},"is_global":false,"sort":{"fields":[{"column":"sequenceNumber","direction":"asc"}]},"joins_events":true,"joins_created":false},{"name":"audio_since","row_rules":[{"field":"mediaKind","test":"equals","value":"AUDIO"},{"field":"sequenceNumber","test":"greater_than","parameter":"afterSequence"}],"joins_node":true,"by_columns":["chatId"],"text_params":["afterSequence"],"scalar_types":{"afterSequence":"int"},"remote":{"operation":"ChatMedia","shape":{"type":"connection","direction":"forward","coverage":"page","max_pages":null,"cursor":null},"stale_time":"5.minutes","empty_stale_time":null,"selects_page_info":true,"page_size":50,"variables":{"includeGifts":{"from":"constant","value":false},"mediaKinds":{"from":"constant","value":["AUDIO"]}},"root":"node.messages"},"is_global":false,"sort":{"fields":[{"column":"sequenceNumber","direction":"asc"}]},"joins_events":true,"joins_created":false},{"name":"gifts_since","row_rules":[{"field":"kind","test":"equals","value":"GIFT"},{"field":"sequenceNumber","test":"greater_than","parameter":"afterSequence"}],"joins_node":true,"by_columns":["chatId"],"text_params":["afterSequence"],"scalar_types":{"afterSequence":"int"},"remote":{"operation":"ChatMedia","shape":{"type":"connection","direction":"forward","coverage":"page","max_pages":null,"cursor":null},"stale_time":"5.minutes","empty_stale_time":null,"selects_page_info":true,"page_size":50,"variables":{"includeGifts":{"from":"constant","value":true}},"root":"node.messages"},"is_global":false,"sort":{"fields":[{"column":"sequenceNumber","direction":"asc"}]},"joins_events":true,"joins_created":false}],"indexes":[["chatId","sequenceNumber"]],"actions":[],"ephemeral":[],"storage":"disk","expire_after":null,"listens":[]},{"key":"Moment","fields":[{"name":"id","kind":{"type":"scalar","kind":"id"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"uuid","kind":{"type":"scalar","kind":"str"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"userId","kind":{"type":"scalar","kind":"id"},"nullable":true,"optional":false,"default":null,"indexed":true,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"text","kind":{"type":"scalar","kind":"str"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null,"searchable":true},{"name":"likedByMe","kind":{"type":"scalar","kind":"bool"},"nullable":false,"optional":false,"default":false,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"media","kind":{"type":"raw"},"nullable":false,"optional":false,"default":{},"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"tone","kind":{"type":"scalar","kind":"str"},"nullable":false,"optional":true,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":{"type":"local"}},{"name":"createdAt","kind":{"type":"scalar","kind":"date"},"nullable":false,"optional":false,"default":null,"indexed":true,"id_from_parent":null,"from":null,"app_type":null,"merge":null}],"associations":[{"name":"author","kind":"belongs_to","target":"User","foreign_key":"userId","dependent":null,"touches_parent":true,"sets":{},"newer":[],"from":null},{"name":"vibes","kind":"has_many","target":"Vibe","foreign_key":"momentId","dependent":"destroy","touches_parent":false,"sets":{},"newer":[],"from":null}],"relations":[{"name":"transcode_status","by_columns":[],"text_params":["nodeId"],"remote":{"operation":"MomentTranscodeStatus","shape":{"type":"single"},"stale_time":"5.minutes","empty_stale_time":null,"selects_page_info":false,"page_size":null,"variables":{"momentId":{"from":"param","name":"nodeId"}},"root":null},"is_global":false,"sort":"server_order","joins_events":false,"joins_created":false,"node":"one"},{"name":"recent","by_columns":[],"text_params":[],"remote":null,"is_global":true,"sort":{"fields":[{"column":"createdAt","direction":"desc"}]},"joins_events":false,"joins_created":false},{"name":"by_user","by_columns":["userId"],"text_params":[],"remote":null,"is_global":false,"sort":{"fields":[{"column":"createdAt","direction":"desc"}]},"joins_events":false,"joins_created":false},{"name":"feed","updates":[{"subscription":"MomentEvents","deleted_by":"destroyed","set":{},"removes":false,"debounce":null,"root":null,"apply":{"effect":"upsert","matches":{}}}],"by_columns":[],"text_params":[],"remote":{"operation":"MomentsFeed","shape":{"type":"connection","direction":"forward","coverage":"page","max_pages":null,"cursor":null},"stale_time":"5.minutes","empty_stale_time":null,"selects_page_info":true,"page_size":20,"variables":{},"root":null},"is_global":true,"sort":"server_order","joins_events":false,"joins_created":false,"edge_fields":[{"name":"impressionUuid","kind":{"type":"scalar","kind":"str"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"impressionScore","kind":{"type":"scalar","kind":"int"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null}]},{"name":"vibe_feed","by_columns":[],"text_params":["vibeId"],"remote":{"operation":"MomentsFeed","shape":{"type":"connection","direction":"forward","coverage":"page","max_pages":null,"cursor":null},"stale_time":"5.minutes","empty_stale_time":null,"selects_page_info":true,"page_size":20,"variables":{},"root":null},"is_global":false,"sort":"server_order","joins_events":false,"joins_created":false,"edge_fields":[{"name":"impressionUuid","kind":{"type":"scalar","kind":"str"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"score","kind":{"type":"scalar","kind":"int"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":"impressionScore","app_type":null,"merge":null},{"name":"rank","kind":{"type":"enum","values":["top","normal"]},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"FeedRank","merge":null}]}],"indexes":[["uuid"],["userId","createdAt"]],"actions":[{"client_mutation_id":false,"name":"create","root_shape":"upsert","on_failure":"rollback","has_optimistic":true,"has_temp_row_retention":true,"has_cascade_dependents":false,"members":[],"mutation":"CreateMoment","upload":false,"variables":{"media":"media","text":"text"},"optimistic":{},"refetch":[],"args":{},"local":[],"correlate":[],"ignore_response":false,"wrap":null,"root":null,"lands":[],"result":{}},{"client_mutation_id":false,"name":"update","root_shape":"upsert","on_failure":"rollback","has_optimistic":false,"has_temp_row_retention":false,"has_cascade_dependents":false,"members":[],"mutation":"UpdateMoment","upload":false,"variables":{},"optimistic":{},"refetch":[],"args":{},"local":[],"correlate":[],"ignore_response":false,"wrap":null,"root":null,"lands":[],"result":{}},{"client_mutation_id":false,"name":"upload_clip","root_shape":"upsert","on_failure":"rollback","has_optimistic":false,"has_temp_row_retention":false,"has_cascade_dependents":false,"members":[],"mutation":"CreateMoment","upload":true,"variables":{"momentId":"id"},"optimistic":{},"refetch":[],"args":{},"local":[],"correlate":[],"ignore_response":false,"wrap":null,"root":"createMoment","lands":[],"result":{"successMessage":"string"}},{"client_mutation_id":false,"name":"destroy","root_shape":"destroy","on_failure":"rollback","has_optimistic":false,"has_temp_row_retention":false,"has_cascade_dependents":false,"members":[],"mutation":"DeleteMoment","upload":false,"variables":{},"optimistic":{},"refetch":[],"args":{},"local":[],"correlate":[],"ignore_response":false,"wrap":null,"root":null,"lands":[],"result":{}},{"client_mutation_id":false,"name":"like","root_shape":"upsert","on_failure":"rollback","has_optimistic":false,"has_temp_row_retention":false,"has_cascade_dependents":false,"members":[],"mutation":"LikeMoment","upload":false,"variables":{"momentId":"id"},"optimistic":{"likedByMe":true},"refetch":["feed"],"args":{},"local":[],"correlate":[],"ignore_response":false,"wrap":null,"root":null,"lands":[],"result":{}},{"client_mutation_id":false,"name":"share","root_shape":"upsert","on_failure":"rollback","has_optimistic":false,"has_temp_row_retention":false,"has_cascade_dependents":false,"members":[],"mutation":"ShareMoment","upload":false,"variables":{"momentId":"id"},"optimistic":{},"refetch":[],"args":{},"local":[],"correlate":[],"ignore_response":false,"wrap":null,"root":"shareMoment.moment","lands":[],"result":{"copies":"int","successMessage":"string"}}],"ephemeral":[],"storage":"disk","expire_after":null,"listens":[],"upload_steps":{"create_blob":{"name":"CreateBlob","root_path":"blobCreate"},"multipart":{"initiate":{"name":"InitMultipart"},"presign":{"name":"PresignParts"},"complete":{"name":"CompleteMultipart"},"abort":{"name":"AbortMultipart"}}}},{"key":"NestedSample","fields":[{"name":"id","kind":{"type":"scalar","kind":"id"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"phase","kind":{"type":"enum","values":["draft","live"]},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"NestedSamplePhase","merge":null},{"name":"value","kind":{"type":"object"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"NestedValue","merge":null,"fields":[{"name":"title","kind":{"type":"scalar","kind":"str"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"note","kind":{"type":"scalar","kind":"str"},"nullable":false,"optional":true,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"nullableDefault","kind":{"type":"scalar","kind":"str"},"nullable":true,"optional":false,"default":"default","indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"ownerId","kind":{"type":"scalar","kind":"id"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":"owner_id","app_type":null,"merge":null},{"name":"count","kind":{"type":"scalar","kind":"int"},"nullable":false,"optional":false,"default":2,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"ratio","kind":{"type":"scalar","kind":"num"},"nullable":false,"optional":false,"default":1.5,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"enabled","kind":{"type":"scalar","kind":"bool"},"nullable":false,"optional":false,"default":false,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"createdAt","kind":{"type":"scalar","kind":"date"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"state","kind":{"type":"enum","values":["ready","done"]},"nullable":false,"optional":false,"default":"ready","indexed":false,"id_from_parent":null,"from":null,"app_type":"NestedValueState","merge":null},{"name":"labels","kind":{"type":"list","element":"str"},"nullable":false,"optional":false,"default":[],"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"counts","kind":{"type":"list","element":"int"},"nullable":false,"optional":false,"default":[],"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"ratios","kind":{"type":"list","element":"num"},"nullable":false,"optional":false,"default":[],"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"flags","kind":{"type":"list","element":"bool"},"nullable":false,"optional":false,"default":[],"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"dates","kind":{"type":"list","element":"date"},"nullable":false,"optional":false,"default":[],"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"owners","kind":{"type":"list","element":"id"},"nullable":false,"optional":false,"default":[],"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"payload","kind":{"type":"raw"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"rawValue","kind":{"type":"raw"},"nullable":false,"optional":false,"default":{"free":[1,true,null]},"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"preferences","kind":{"type":"object"},"nullable":false,"optional":false,"default":{"label":"base"},"indexed":false,"id_from_parent":null,"from":null,"app_type":"NestedValuePreferences","merge":null,"fields":[{"name":"label","kind":{"type":"scalar","kind":"str"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"active","kind":{"type":"scalar","kind":"bool"},"nullable":false,"optional":false,"default":true,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null}]},{"name":"presets","kind":{"type":"array"},"nullable":false,"optional":false,"default":[{"label":"first"}],"indexed":false,"id_from_parent":null,"from":null,"app_type":"NestedValuePreferences","merge":null},{"name":"metadata","kind":{"type":"raw"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"values","kind":{"type":"raw"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"child","kind":{"type":"object"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"NestedValueChild","merge":null,"fields":[{"name":"body","kind":{"type":"scalar","kind":"str"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"horizontal","kind":{"type":"scalar","kind":"num"},"nullable":false,"optional":false,"default":0,"indexed":false,"id_from_parent":null,"from":"x","app_type":null,"merge":null}]},{"name":"children","kind":{"type":"array"},"nullable":false,"optional":false,"default":[],"indexed":false,"id_from_parent":null,"from":null,"app_type":"NestedValueChild","merge":null}]},{"name":"values","kind":{"type":"array"},"nullable":false,"optional":false,"default":[],"indexed":false,"id_from_parent":null,"from":null,"app_type":"NestedValue","merge":null},{"name":"tree","kind":{"type":"object"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"NestedTree","merge":null,"fields":[{"name":"name","kind":{"type":"scalar","kind":"str"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"children","kind":{"type":"array"},"nullable":false,"optional":false,"default":[],"indexed":false,"id_from_parent":null,"from":null,"app_type":"NestedTree","merge":null}]},{"name":"replyTo","kind":{"type":"object"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"RecordReplyNode","merge":null,"fields":[{"name":"id","kind":{"type":"scalar","kind":"id"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"body","kind":{"type":"scalar","kind":"str"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"kind","kind":{"type":"scalar","kind":"str"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"userId","kind":{"type":"scalar","kind":"id"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"user","kind":{"type":"object"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"RecordUserRef","merge":null,"fields":[{"name":"id","kind":{"type":"scalar","kind":"id"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"fullName","kind":{"type":"scalar","kind":"str"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"name","kind":{"type":"scalar","kind":"str"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"username","kind":{"type":"scalar","kind":"str"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"avatarUrl","kind":{"type":"scalar","kind":"str"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null}]},{"name":"media","kind":{"type":"object"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"NestedMedia","merge":null,"fields":[{"name":"id","kind":{"type":"scalar","kind":"id"},"nullable":false,"optional":false,"default":"","indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"fileUrl","kind":{"type":"scalar","kind":"str"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null}]}]}],"associations":[],"relations":[],"indexes":[],"actions":[],"ephemeral":[],"storage":"local","expire_after":null,"listens":[]},{"key":"NestedReference","fields":[{"name":"id","kind":{"type":"scalar","kind":"id"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"values","kind":{"type":"array"},"nullable":false,"optional":false,"default":[],"indexed":false,"id_from_parent":null,"from":null,"app_type":"NestedValue","merge":null}],"associations":[],"relations":[],"indexes":[],"actions":[],"ephemeral":[],"storage":"local","expire_after":null,"listens":[]},{"key":"Notice","fields":[{"name":"id","kind":{"type":"scalar","kind":"id"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"body","kind":{"type":"scalar","kind":"str"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"occurredAt","kind":{"type":"scalar","kind":"date"},"nullable":false,"optional":false,"default":null,"indexed":true,"id_from_parent":null,"from":null,"app_type":null,"merge":null}],"associations":[],"relations":[{"name":"recent","updates":[{"subscription":"BulletinAdded","deleted_by":null,"set":{},"removes":false,"debounce":null,"root":null,"apply":{"effect":"upsert","matches":{}}}],"by_columns":[],"text_params":[],"remote":null,"is_global":true,"sort":{"fields":[{"column":"occurredAt","direction":"desc"}]},"joins_events":false,"joins_created":false}],"indexes":[],"actions":[],"ephemeral":[],"storage":"memory","expire_after":"1.minute","listens":[]},{"key":"InteractionNotice","fields":[{"name":"id","kind":{"type":"scalar","kind":"id"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"recordId","kind":{"type":"scalar","kind":"id"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"actorUserId","kind":{"type":"scalar","kind":"id"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"occurredAt","kind":{"type":"scalar","kind":"date"},"nullable":false,"optional":false,"default":null,"indexed":true,"id_from_parent":null,"from":null,"app_type":null,"merge":null}],"associations":[],"relations":[{"name":"recent","updates":[{"subscription":"RecordSignal","deleted_by":null,"set":{},"removes":false,"debounce":null,"root":null,"apply":{"effect":"upsert","matches":{}}}],"by_columns":[],"text_params":[],"remote":null,"is_global":true,"sort":{"fields":[{"column":"occurredAt","direction":"desc"}]},"joins_events":false,"joins_created":false}],"indexes":[],"actions":[],"ephemeral":[],"storage":"memory","expire_after":"1.minute","listens":[]},{"key":"ChatAction","fields":[{"name":"id","kind":{"type":"scalar","kind":"id"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"chatId","kind":{"type":"scalar","kind":"id"},"nullable":false,"optional":false,"default":null,"indexed":true,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"userId","kind":{"type":"scalar","kind":"id"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"kind","kind":{"type":"scalar","kind":"str"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"active","kind":{"type":"scalar","kind":"bool"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null}],"associations":[],"relations":[{"name":"by_chat","updates":[{"subscription":"ChatActionSignal","deleted_by":null,"deleted_unless":"active","set":{},"removes":false,"debounce":null,"root":null,"id":["chatId","userId"],"apply":{"effect":"upsert","matches":{"kind":["RECORDING_AUDIO","RECORDING_VIDEO","TYPING","UPLOADING_PHOTO","UPLOADING_VIDEO"]}}}],"by_columns":["chatId"],"text_params":[],"remote":null,"is_global":false,"sort":"server_order","joins_events":false,"joins_created":false}],"indexes":[],"actions":[],"ephemeral":[],"storage":"memory","expire_after":"8.seconds","listens":[]},{"key":"ChatActivityNotice","fields":[{"name":"id","kind":{"type":"scalar","kind":"id"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"chatId","kind":{"type":"scalar","kind":"id"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"userId","kind":{"type":"scalar","kind":"id"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"kind","kind":{"type":"scalar","kind":"str"},"nullable":false,"optional":false,"default":null,"indexed":true,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"active","kind":{"type":"scalar","kind":"bool"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null}],"associations":[],"relations":[{"name":"recent","by_columns":[],"text_params":[],"remote":null,"is_global":true,"sort":{"fields":[{"column":"kind","direction":"asc"}]},"joins_events":false,"joins_created":false}],"indexes":[],"actions":[],"ephemeral":[],"storage":"memory","expire_after":"1.minute","listens":[{"subscription":"ChatActionSignal","deleted_by":null,"deleted_unless":"active","set":{},"removes":false,"debounce":null,"root":null,"id":["chatId","userId"],"apply":{"effect":"upsert","matches":{"kind":["SCREENSHOT"]}}}]},{"key":"Presence","fields":[{"name":"id","kind":{"type":"scalar","kind":"id"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"userId","kind":{"type":"scalar","kind":"id"},"nullable":false,"optional":false,"default":null,"indexed":true,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"room","kind":{"type":"scalar","kind":"str"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null}],"associations":[{"name":"user","kind":"belongs_to","target":"User","foreign_key":"userId","dependent":null,"touches_parent":false,"sets":{},"newer":[],"from":null}],"relations":[{"name":"for_user","by_columns":["userId"],"text_params":[],"remote":null,"is_global":false,"sort":"server_order","joins_events":false,"joins_created":false},{"name":"online","updates":[{"subscription":"PresenceChanged","deleted_by":"left","set":{},"removes":false,"debounce":null,"root":null,"apply":{"effect":"upsert","matches":{}}}],"by_columns":[],"text_params":[],"remote":{"operation":"PresenceOnline","shape":{"type":"list","coverage":"complete"},"stale_time":"30.seconds","empty_stale_time":null,"selects_page_info":false,"page_size":null,"variables":{},"root":null},"is_global":true,"sort":"server_order","joins_events":false,"joins_created":false,"preload":{}},{"name":"on_channel","by_columns":[],"text_params":["channel"],"remote":{"operation":"PresenceOnChannel","shape":{"type":"list","coverage":"complete"},"stale_time":"5.minutes","empty_stale_time":null,"selects_page_info":false,"page_size":null,"variables":{},"root":null},"is_global":false,"sort":"server_order","joins_events":false,"joins_created":false,"preload":{"channel":["lobby","stage"]},"preload_order":["channel"]},{"name":"history","by_columns":[],"text_params":["presenceId"],"remote":{"operation":"PresenceHistory","shape":{"type":"list","coverage":"complete"},"stale_time":"12.hours","empty_stale_time":null,"selects_page_info":false,"page_size":null,"variables":{},"root":null},"is_global":false,"sort":"server_order","joins_events":false,"joins_created":false,"preload":{"presenceId":{"from":{"model":"Presence","relation":"online"}}},"preload_limit":40,"preload_order":["presenceId"]}],"indexes":[],"actions":[],"ephemeral":[],"storage":"memory","expire_after":"1.minute","listens":[]},{"key":"Record","fields":[{"name":"id","kind":{"type":"scalar","kind":"id"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"containerId","kind":{"type":"scalar","kind":"id"},"nullable":false,"optional":false,"default":null,"indexed":true,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"authorId","kind":{"type":"scalar","kind":"id"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"regionId","kind":{"type":"scalar","kind":"id"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"text","kind":{"type":"scalar","kind":"str"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"createdAt","kind":{"type":"scalar","kind":"date"},"nullable":false,"optional":false,"default":null,"indexed":true,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"sequenceNumber","kind":{"type":"scalar","kind":"int"},"nullable":true,"optional":false,"default":null,"indexed":true,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"reactions","kind":{"type":"array"},"nullable":false,"optional":false,"default":[],"indexed":false,"id_from_parent":null,"from":null,"app_type":"RecordReactionNode","merge":null,"fields":[{"name":"body","kind":{"type":"scalar","kind":"str"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"userId","kind":{"type":"scalar","kind":"id"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":"user.id","app_type":null,"merge":null},{"name":"user","kind":{"type":"object"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"link":{"model":"User","id":"userId"},"app_type":"RecordReactionUser","merge":null,"fields":[{"name":"id","kind":{"type":"scalar","kind":"id"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"name","kind":{"type":"scalar","kind":"str"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null}]}]},{"name":"attachment","kind":{"type":"union"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"RecordAttachment","merge":null,"fields":[{"name":"reward","kind":{"type":"object"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"RewardAttachment","merge":null,"fields":[{"name":"id","kind":{"type":"scalar","kind":"id"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"tier","kind":{"type":"scalar","kind":"str"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":"rewardTier","app_type":null,"merge":null},{"name":"status","kind":{"type":"scalar","kind":"str"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"tags","kind":{"type":"list","element":"str"},"nullable":false,"optional":false,"default":[],"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null}]},{"name":"moment","kind":{"type":"object"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"MomentAttachment","merge":null,"fields":[{"name":"momentId","kind":{"type":"scalar","kind":"id"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"caption","kind":{"type":"scalar","kind":"str"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null}]},{"name":"profile","kind":{"type":"object"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"ProfileAttachment","merge":null,"fields":[{"name":"user","kind":{"type":"object"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"AttachmentUser","merge":null,"fields":[{"name":"id","kind":{"type":"scalar","kind":"id"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"name","kind":{"type":"scalar","kind":"str"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null}]}]},{"name":"reaction","kind":{"type":"object"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"ReactionAttachment","merge":null,"fields":[{"name":"body","kind":{"type":"scalar","kind":"str"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null}]}]},{"name":"media","kind":{"type":"object"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"RecordMedia","merge":{"type":"keys","keys":{"fileUrl":{"type":"non_empty"}}},"fields":[{"name":"kind","kind":{"type":"scalar","kind":"str"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"transcoded","kind":{"type":"scalar","kind":"bool"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"fileUrl","kind":{"type":"scalar","kind":"str"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null}]}],"associations":[{"name":"container","kind":"belongs_to","target":"Container","foreign_key":"containerId","dependent":null,"touches_parent":false,"sets":{"lastRecordAt":"createdAt","lastRecordId":"id"},"newer":[{"child":"createdAt","parent":"lastRecordAt"}],"from":null}],"relations":[{"name":"by_container","by_columns":["containerId"],"text_params":[],"remote":null,"is_global":false,"sort":{"fields":[{"column":"createdAt","direction":"asc"}]},"joins_events":false,"joins_created":false},{"name":"thread","by_columns":["containerId"],"text_params":[],"remote":{"operation":"ContainerThread","shape":{"type":"connection","direction":"forward","coverage":"page","max_pages":null,"cursor":null},"stale_time":"5.minutes","empty_stale_time":null,"selects_page_info":true,"page_size":30,"variables":{},"root":"container.records"},"is_global":false,"sort":{"fields":[{"column":"sequenceNumber","direction":"asc_nulls_last"},{"column":"createdAt","direction":"asc"}]},"joins_events":true,"joins_created":false,"order_stable":true},{"name":"node","by_columns":[],"text_params":["nodeId"],"remote":{"operation":"RecordNode","shape":{"type":"single"},"stale_time":"5.minutes","empty_stale_time":null,"selects_page_info":false,"page_size":null,"variables":{"id":{"from":"param","name":"nodeId"}},"root":null},"is_global":false,"sort":"server_order","joins_events":false,"joins_created":false,"node":"one"},{"name":"transcode_status","by_columns":[],"text_params":["nodeId"],"remote":{"operation":"RecordTranscodeStatus","shape":{"type":"single"},"stale_time":"5.minutes","empty_stale_time":null,"selects_page_info":false,"page_size":null,"variables":{"id":{"from":"param","name":"nodeId"}},"root":null},"is_global":false,"sort":"server_order","joins_events":false,"joins_created":false,"node":"one"},{"name":"container_media","by_columns":["containerId"],"text_params":[],"remote":{"operation":"ContainerMedia","shape":{"type":"connection","direction":"forward","coverage":"page","max_pages":null,"cursor":null},"stale_time":"5.minutes","empty_stale_time":null,"selects_page_info":true,"page_size":30,"variables":{},"root":"node.records"},"is_global":false,"sort":"server_order","joins_events":false,"joins_created":false},{"name":"history","by_columns":[],"text_params":[],"remote":{"operation":"ContainerRecords","shape":{"type":"connection","direction":"backward","coverage":"page","max_pages":null,"cursor":null},"stale_time":"1.minute","empty_stale_time":"10.seconds","selects_page_info":true,"page_size":30,"variables":{},"root":null},"is_global":true,"sort":"server_order","joins_events":false,"joins_created":false}],"indexes":[],"actions":[{"client_mutation_id":false,"name":"create","root_shape":"upsert","on_failure":"rollback","has_optimistic":true,"has_temp_row_retention":true,"has_cascade_dependents":false,"members":[],"mutation":"CreateRecord","upload":false,"variables":{"attachedUserId":"attachedUserId","containerId":"containerId","signedId":"signedId","text":"text"},"optimistic":{},"refetch":[],"args":{"attachedUserId":"id?","signedId":"string?"},"local":["authorId","regionId"],"local_from":{"authorId":{"row":{"model":"CurrentUser","field":"id"}},"regionId":{"row":{"model":"CurrentRegion","field":"id"}}},"correlate":[],"ignore_response":false,"wrap":null,"root":null,"lands":[],"result":{}},{"client_mutation_id":false,"name":"react","root_shape":"upsert","on_failure":"rollback","has_optimistic":false,"has_temp_row_retention":false,"has_cascade_dependents":false,"members":[],"mutation":"RecordReact","upload":false,"variables":{"recordId":"id"},"optimistic":{"reactions":{"$keyed":{"by":"userId","element":{"body":{"$arg":"reactionBody"},"userId":{"$arg":"userId"}}}}},"refetch":[],"args":{"reactionBody":"string","userId":"id"},"local":[],"correlate":[],"ignore_response":false,"wrap":null,"root":null,"lands":[],"result":{}}],"ephemeral":[],"storage":"disk","expire_after":null,"listens":[],"changes":[{"area":"RECORD","relation":"observed","parameter":null,"observed":true}]},{"key":"CurrentUser","fields":[{"name":"id","kind":{"type":"scalar","kind":"id"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"name","kind":{"type":"scalar","kind":"str"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null}],"associations":[],"relations":[{"name":"current","by_columns":[],"text_params":[],"remote":{"operation":"CurrentUserQuery","shape":{"type":"single"},"stale_time":"5.minutes","empty_stale_time":null,"selects_page_info":false,"page_size":null,"variables":{},"root":null},"is_global":true,"sort":"server_order","joins_events":false,"joins_created":false}],"indexes":[],"actions":[],"ephemeral":[],"storage":"disk","expire_after":null,"listens":[]},{"key":"CurrentRegion","fields":[{"name":"id","kind":{"type":"scalar","kind":"id"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"name","kind":{"type":"scalar","kind":"str"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null}],"associations":[],"relations":[{"name":"current","by_columns":[],"text_params":[],"remote":{"operation":"CurrentRegionQuery","shape":{"type":"single"},"stale_time":"5.minutes","empty_stale_time":null,"selects_page_info":false,"page_size":null,"variables":{},"root":null},"is_global":true,"sort":"server_order","joins_events":false,"joins_created":false}],"indexes":[],"actions":[],"ephemeral":[],"storage":"disk","expire_after":null,"listens":[]},{"key":"CustomScreen","fields":[{"name":"id","kind":{"type":"scalar","kind":"id"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"slug","kind":{"type":"scalar","kind":"str"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"version","kind":{"type":"scalar","kind":"int"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"requires","kind":{"type":"object"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"ScreenRequires","merge":null,"fields":[{"name":"ios","kind":{"type":"scalar","kind":"int"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"android","kind":{"type":"scalar","kind":"int"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null}]},{"name":"spec","kind":{"type":"object"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"ScreenSpec","merge":null,"fields":[{"name":"root","kind":{"type":"scalar","kind":"id"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"state","kind":{"type":"array"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"ScreenStateEntry","merge":null,"fields":[{"name":"path","kind":{"type":"scalar","kind":"str"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"value","kind":{"type":"raw"},"parse_json":true,"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null}]},{"name":"elements","kind":{"type":"array"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"ScreenElement","merge":null,"fields":[{"name":"id","kind":{"type":"scalar","kind":"id"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"type","kind":{"type":"scalar","kind":"str"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"slot","kind":{"type":"scalar","kind":"str"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"children","kind":{"type":"list","element":"id"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"props","kind":{"type":"raw"},"parse_json":true,"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"visible","kind":{"type":"raw"},"parse_json":true,"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"press","kind":{"type":"raw"},"parse_json":true,"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"change","kind":{"type":"raw"},"parse_json":true,"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"padding","kind":{"type":"scalar","kind":"str"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"margin","kind":{"type":"scalar","kind":"str"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"background","kind":{"type":"scalar","kind":"str"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"radius","kind":{"type":"scalar","kind":"str"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"expand","kind":{"type":"scalar","kind":"bool"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"align","kind":{"type":"scalar","kind":"str"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"accessibilityLabel","kind":{"type":"scalar","kind":"str"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null}]}]}],"associations":[],"relations":[{"name":"node","by_columns":[],"text_params":["nodeId"],"remote":{"operation":"ScreenNode","shape":{"type":"single"},"stale_time":"5.minutes","empty_stale_time":null,"selects_page_info":false,"page_size":null,"variables":{"id":{"from":"param","name":"nodeId"}},"root":null},"is_global":false,"sort":"server_order","joins_events":false,"joins_created":false,"node":"one"}],"indexes":[],"actions":[],"ephemeral":[],"storage":"disk","expire_after":null,"listens":[]},{"key":"StorageRow","fields":[{"name":"id","kind":{"type":"scalar","kind":"id"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"record","kind":{"type":"scalar","kind":"str"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null}],"associations":[],"relations":[],"indexes":[],"actions":[],"ephemeral":[],"storage":"disk","expire_after":null,"listens":[]},{"key":"Tail","fields":[{"name":"id","kind":{"type":"scalar","kind":"id"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"text","kind":{"type":"scalar","kind":"str"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null}],"associations":[],"relations":[{"name":"since","by_columns":[],"text_params":["containerId","afterSequence","pinned"],"scalar_types":{"afterSequence":"int","pinned":"bool"},"remote":{"operation":"tailSince","shape":{"type":"connection","direction":"forward","coverage":"page","max_pages":null,"cursor":null},"stale_time":"5.minutes","empty_stale_time":null,"selects_page_info":true,"page_size":50,"variables":{},"root":"tailSince"},"is_global":false,"sort":"server_order","joins_events":false,"joins_created":false}],"indexes":[],"actions":[],"ephemeral":[],"storage":"disk","expire_after":null,"listens":[]},{"key":"UnionSample","fields":[{"name":"id","kind":{"type":"scalar","kind":"id"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"label","kind":{"type":"scalar","kind":"str"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"attachment","kind":{"type":"union"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"RecordAttachment","merge":null},{"name":"props","kind":{"type":"union"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"MomentWidgetItemProps","merge":null,"fields":[{"name":"emoji","kind":{"type":"object"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"MomentEmojiProps","merge":null,"fields":[{"name":"emoji","kind":{"type":"scalar","kind":"str"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null}]},{"name":"label","kind":{"type":"object"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"MomentLabelProps","merge":null,"fields":[{"name":"text","kind":{"type":"scalar","kind":"str"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"size","kind":{"type":"scalar","kind":"int"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null}]}]}],"associations":[],"relations":[],"indexes":[],"actions":[],"ephemeral":[],"storage":"local","expire_after":null,"listens":[]},{"key":"User","fields":[{"name":"id","kind":{"type":"scalar","kind":"id"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"name","kind":{"type":"scalar","kind":"str"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"avatar","kind":{"type":"file"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"birthday","kind":{"type":"scalar","kind":"date"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null}],"associations":[],"relations":[{"name":"profile","updates":[{"subscription":"UserTyping","deleted_by":null,"set":{"typing":true},"removes":false,"debounce":null,"root":null,"apply":{"effect":"upsert","matches":{}}}],"by_columns":[],"text_params":[],"remote":{"operation":"UserProfile","shape":{"type":"single"},"stale_time":"5.minutes","empty_stale_time":null,"selects_page_info":false,"page_size":null,"variables":{},"root":null},"is_global":true,"sort":"server_order","joins_events":false,"joins_created":false},{"name":"container_members","by_columns":[],"text_params":["containerId"],"remote":{"operation":"ContainerMembers","shape":{"type":"connection","direction":"forward","coverage":"page","max_pages":null,"cursor":null},"stale_time":"5.minutes","empty_stale_time":null,"selects_page_info":true,"page_size":30,"variables":{},"root":"node.members","total_from":"node.membersCount"},"is_global":false,"sort":"server_order","joins_events":false,"joins_created":false},{"name":"complete_members","by_columns":[],"text_params":["containerId"],"remote":{"operation":"ContainerMembers","shape":{"type":"connection","direction":"forward","coverage":"page","max_pages":null,"cursor":null},"stale_time":"5.minutes","empty_stale_time":null,"selects_page_info":true,"page_size":100,"variables":{},"root":"node.members","total_from":"node.membersCount"},"is_global":false,"sort":"server_order","joins_events":false,"joins_created":false,"members_complete":true}],"indexes":[],"actions":[],"ephemeral":[{"name":"typing","kind":{"type":"scalar","kind":"bool"},"nullable":false,"optional":false,"default":false,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null,"reset_after":"5.seconds"},{"name":"mood","kind":{"type":"scalar","kind":"str"},"nullable":true,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null,"reset_after":null}],"storage":"disk","expire_after":null,"listens":[]},{"key":"Vibe","fields":[{"name":"id","kind":{"type":"scalar","kind":"id"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"momentId","kind":{"type":"scalar","kind":"id"},"nullable":false,"optional":false,"default":null,"indexed":true,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"userId","kind":{"type":"scalar","kind":"id"},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":null,"merge":null},{"name":"kind","kind":{"type":"enum","values":["like","love","laugh"]},"nullable":false,"optional":false,"default":null,"indexed":false,"id_from_parent":null,"from":null,"app_type":"VibeKind","merge":null}],"associations":[],"relations":[],"indexes":[["userId"]],"actions":[],"ephemeral":[],"storage":"disk","expire_after":null,"listens":[]}],"operations":[{"name":"KeywordCall","returns":{"app_type":"WhenResult","default":null,"fields":[{"app_type":null,"default":null,"from":null,"id_from_parent":null,"indexed":false,"kind":{"kind":"str","type":"scalar"},"merge":null,"name":"default","nullable":true,"optional":false},{"app_type":null,"default":null,"from":null,"id_from_parent":null,"indexed":false,"kind":{"kind":"str","type":"scalar"},"merge":null,"name":"when","nullable":true,"optional":false},{"app_type":null,"default":null,"from":null,"id_from_parent":null,"indexed":false,"kind":{"kind":"str","type":"scalar"},"merge":null,"name":"container","nullable":true,"optional":false},{"app_type":null,"default":null,"from":null,"id_from_parent":null,"indexed":false,"kind":{"kind":"str","type":"scalar"},"merge":null,"name":"r","nullable":true,"optional":false}],"from":null,"id_from_parent":null,"indexed":false,"kind":{"type":"object"},"merge":null,"name":"when","nullable":false,"optional":false}},{"name":"QrCodeCreate","returns":{"app_type":"QrCodeCreateResult","default":null,"fields":[{"app_type":null,"default":null,"from":null,"id_from_parent":null,"indexed":false,"kind":{"kind":"str","type":"scalar"},"merge":null,"name":"url","nullable":true,"optional":false}],"from":null,"id_from_parent":null,"indexed":false,"kind":{"type":"object"},"merge":null,"name":"qr_code_create","nullable":false,"optional":false}},{"name":"SignOut","returns":{"app_type":null,"default":null,"from":null,"id_from_parent":null,"indexed":false,"kind":{"type":"raw"},"merge":null,"name":"sign_out","nullable":false,"optional":false}},{"name":"StickerSend","returns":{"app_type":"StickerSendResult","default":null,"fields":[{"app_type":null,"default":null,"from":null,"id_from_parent":null,"indexed":false,"kind":{"kind":"str","type":"scalar"},"merge":null,"name":"id","nullable":true,"optional":false}],"from":null,"id_from_parent":null,"indexed":false,"kind":{"type":"object"},"merge":null,"name":"sticker_send","nullable":false,"optional":false}},{"name":"Pricing","returns":{"app_type":"PricingLoadResult","default":null,"fields":[{"app_type":"PricingLoadResultRewards","default":null,"fields":[{"app_type":null,"default":null,"from":null,"id_from_parent":null,"indexed":false,"kind":{"kind":"id","type":"scalar"},"merge":null,"name":"id","nullable":true,"optional":false},{"app_type":null,"default":null,"from":null,"id_from_parent":null,"indexed":false,"kind":{"kind":"str","type":"scalar"},"merge":null,"name":"name","nullable":true,"optional":false},{"app_type":null,"default":null,"from":null,"id_from_parent":null,"indexed":false,"kind":{"kind":"int","type":"scalar"},"merge":null,"name":"price","nullable":true,"optional":false}],"from":null,"id_from_parent":null,"indexed":false,"kind":{"type":"array"},"merge":null,"name":"rewards","nullable":true,"optional":false},{"app_type":"PricingLoadResultVideoIntro","default":null,"fields":[{"app_type":null,"default":null,"from":null,"id_from_parent":null,"indexed":false,"kind":{"kind":"bool","type":"scalar"},"merge":null,"name":"enabled","nullable":true,"optional":false}],"from":null,"id_from_parent":null,"indexed":false,"kind":{"type":"object"},"merge":null,"name":"videoIntro","nullable":true,"optional":false},{"app_type":null,"default":null,"from":null,"id_from_parent":null,"indexed":false,"kind":{"element":"str","type":"list"},"merge":null,"name":"tags","nullable":true,"optional":false},{"app_type":null,"default":null,"from":null,"id_from_parent":null,"indexed":false,"kind":{"type":"raw"},"merge":null,"name":"spec","nullable":true,"optional":false}],"from":null,"id_from_parent":null,"indexed":false,"kind":{"type":"object"},"merge":null,"name":"pricing_load","nullable":false,"optional":false}},{"name":"MomentCreate","returns":{"app_type":"MomentCreateResult","default":null,"fields":[{"app_type":"MomentCreateResultMoment","default":null,"fields":[{"app_type":null,"default":null,"from":null,"id_from_parent":null,"indexed":false,"kind":{"kind":"id","type":"scalar"},"merge":null,"name":"id","nullable":true,"optional":false}],"from":null,"id_from_parent":null,"indexed":false,"kind":{"type":"object"},"merge":null,"name":"moment","nullable":true,"optional":false}],"from":null,"id_from_parent":null,"indexed":false,"kind":{"type":"object"},"merge":null,"name":"moment_create","nullable":false,"optional":false}},{"name":"Giphy","returns":{"app_type":"GiphyLoadResult","default":null,"fields":[{"app_type":null,"default":null,"from":null,"id_from_parent":null,"indexed":false,"kind":{"kind":"id","type":"scalar"},"merge":null,"name":"id","nullable":true,"optional":false},{"app_type":null,"default":null,"from":null,"id_from_parent":null,"indexed":false,"kind":{"kind":"str","type":"scalar"},"merge":null,"name":"url","nullable":true,"optional":false},{"app_type":null,"default":null,"from":null,"id_from_parent":null,"indexed":false,"kind":{"kind":"bool","type":"scalar"},"merge":null,"name":"isSticker","nullable":true,"optional":false}],"from":null,"id_from_parent":null,"indexed":false,"kind":{"type":"object"},"merge":null,"name":"giphy_load","nullable":false,"optional":false}},{"name":"WidgetsSend","returns":{"app_type":"WidgetsSendResult","default":null,"fields":[{"app_type":null,"default":null,"from":null,"id_from_parent":null,"indexed":false,"kind":{"kind":"str","type":"scalar"},"merge":null,"name":"id","nullable":true,"optional":false}],"from":null,"id_from_parent":null,"indexed":false,"kind":{"type":"object"},"merge":null,"name":"widgets_send","nullable":false,"optional":false}},{"name":"containerMedia","returns":{"app_type":"ContainerMediaLoadResult","default":null,"fields":[{"app_type":null,"default":null,"from":null,"id_from_parent":null,"indexed":false,"kind":{"kind":"int","type":"scalar"},"merge":null,"name":"visualCount","nullable":true,"optional":false},{"app_type":null,"default":null,"from":null,"id_from_parent":null,"indexed":false,"kind":{"kind":"int","type":"scalar"},"merge":null,"name":"audioCount","nullable":true,"optional":false},{"app_type":null,"default":null,"from":null,"id_from_parent":null,"indexed":false,"kind":{"kind":"int","type":"scalar"},"merge":null,"name":"rewardCount","nullable":true,"optional":false},{"app_type":null,"default":null,"from":null,"id_from_parent":null,"indexed":false,"kind":{"kind":"int","type":"scalar"},"merge":null,"name":"latestSequence","nullable":true,"optional":false}],"from":null,"id_from_parent":null,"indexed":false,"kind":{"type":"object"},"merge":null,"name":"container_media_load","nullable":false,"optional":false}}]}"#
