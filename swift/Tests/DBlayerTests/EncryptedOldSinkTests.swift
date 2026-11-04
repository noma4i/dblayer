import DBlayerTesting
import Foundation
import Testing
@testable import DBlayer

private final class CableCredentials: CredentialStore, @unchecked Sendable {
  private let lock = NSLock()
  private var value: String?
  func get() throws -> String? { lock.withLock { value } }
  func set(credential: String) throws { lock.withLock { value = credential } }
  func clear() throws { lock.withLock { value = nil } }
}

private final class CableRows: ValueSink {
  func available() {}
}

private struct CableLogin: Encodable, Sendable { let account: String }

extension BridgeProbeTests {
  @MainActor
  @Test(.timeLimit(.minutes(1)))
  func oldEncryptedCableCannotDeliverIntoTheNextAccount() async throws {
    try await DB.close()
    let server = try SealedServer { path, body in
      if path == "/api/auth/login_link" { return ["sessionId": body["account"] as? String ?? ""] }
      return ["data": [:]]
    }
    let peer = EncryptedCablePeer(server: server)
    let encrypted = EncryptedConfig(origin: SealedServer.origin, serverKey: SealedServer.serverKey, pin: SealedServer.pin, endpoints: EncryptedEndpoints(graphql: "/api/graphql", cable: "/api/cable", logout: "/api/logout", bootstrap: "/api/bootstrap", sessionOperations: ["login_link": EncryptedSessionOperation(path: "/api/auth/login_link", sessionless: false)]), protocolLabel: "example-ws-v1", cablePurpose: "ws.hello", cableChannel: "EncryptedGraphqlChannel")
    try await DB.openTesting(transport: peer, modelsJSON: modelsJSON, encrypted: encrypted, credentials: CableCredentials(), http: server)
    try DB.database.registerOperations(defsJson: #"[{"name":"BulletinAdded","document":"subscription BulletinAdded { bulletinAdded { body occurredAt } }","rootPath":"bulletinAdded"}]"#)
    try await DB.sessionCall("login_link", body: CableLogin(account: "A"))
    let query = #"{"model":"Notice","relation":"recent","params":{}}"#
    let first = try DB.database.subscribe(epoch: DB.database.sessionEpoch(), request: .query(queryJson: query), sink: CableRows())
    let subscriptions = Cursor(peer.subscribed.stream)
    let old = try #require(await subscriptions.next("the first subscription"))
    try await DB.sessionReset()
    #expect(await Cursor(peer.closed.stream).next("the first socket closes") == old)
    try await DB.sessionCall("login_link", body: CableLogin(account: "B"))
    let second = try DB.database.subscribe(epoch: DB.database.sessionEpoch(), request: .query(queryJson: query), sink: CableRows())
    let current = try #require(await subscriptions.next("the second subscription"))
    #expect(current != old)
    try peer.emitOld(token: old)
    let rows = try await DB.database.readQuery(epoch: DB.database.sessionEpoch(), queryJson: query)
    #expect(rows.isEmpty)
    try DB.database.cancelStream(stream: first)
    try DB.database.cancelStream(stream: second)
  }
}
