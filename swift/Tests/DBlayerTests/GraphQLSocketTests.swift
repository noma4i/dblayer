import CryptoKit
import DBlayerTesting
import Foundation
import Network
import Testing
@testable import DBlayer

private final class GraphQLPeer: @unchecked Sendable {
  enum Script { case cable, cableClose, refuseHello(SealedServer) }
  private let listener: NWListener
  private let script: Script
  private let lock = NSLock()
  private var connection: NWConnection?
  let received = AsyncStream<String>.makeStream()

  init(script: Script) throws {
    self.script = script
    let socket = NWProtocolWebSocket.Options()
    socket.setClientRequestHandler(.global()) { _, _ in
      NWProtocolWebSocket.Response(status: .accept, subprotocol: nil)
    }
    let parameters = NWParameters.tcp
    parameters.defaultProtocolStack.applicationProtocols.insert(socket, at: 0)
    listener = try NWListener(using: parameters, on: .any)
  }

  func start() async throws -> URL {
    listener.newConnectionHandler = { [self] connection in
      lock.withLock { self.connection = connection }
      connection.start(queue: .global())
      if case .refuseHello = script {
        let key = Curve25519.KeyAgreement.PrivateKey()
        let challenge = Self.base64URL(Data(repeating: 5, count: 32))
        let publicKey = Self.base64URL(key.publicKey.rawRepresentation)
        send(#"{"type":"challenge","v":1,"kid":"\#(SealedServer.pin)","challenge":"\#(challenge)","x25519":"\#(publicKey)"}"#, on: connection)
      }
      receive(connection)
    }
    try await startListening(listener)
    let port = try #require(listener.port)
    return try #require(URL(string: "http://127.0.0.1:\(port.rawValue)/graphql"))
  }

  func stop() {
    listener.cancel()
    listener.newConnectionHandler = nil
    lock.withLock { connection }?.cancel()
  }

  private func receive(_ connection: NWConnection) {
    connection.receiveMessage { [self] data, context, _, error in
      guard error == nil, let data else { return }
      if let metadata = context?.protocolMetadata(definition: NWProtocolWebSocket.definition) as? NWProtocolWebSocket.Metadata, metadata.opcode == .close { return }
      received.continuation.yield(String(decoding: data, as: UTF8.self))
      if case let .refuseHello(server) = script {
        do {
          let frame = try #require(JSONSerialization.jsonObject(with: data) as? [String: Any])
          if frame["command"] as? String == "subscribe" {
            send(#"{"type":"confirm_subscription"}"#, on: connection)
            receive(connection)
          } else {
            let message = try #require(frame["data"] as? String)
            let body = try #require(JSONSerialization.jsonObject(with: Data(message.utf8)) as? [String: Any])
            _ = try server.open(try #require(body["hello"] as? String))
            close("session_unknown", on: connection)
          }
        } catch { Issue.record("invalid hello: \(error)") }
        return
      }
      send("server frame", on: connection)
      if case .cableClose = script {
        close("UNAUTHENTICATED", on: connection)
      } else {
        receive(connection)
      }
    }
  }

  private static func base64URL(_ data: Data) -> String {
    data.base64EncodedString().replacingOccurrences(of: "+", with: "-").replacingOccurrences(of: "/", with: "_").replacingOccurrences(of: "=", with: "")
  }

  private func close(_ reason: String, on connection: NWConnection) {
    let metadata = NWProtocolWebSocket.Metadata(opcode: .close)
    metadata.closeCode = .privateCode(4401)
    let context = NWConnection.ContentContext(identifier: "close", metadata: [metadata])
    connection.send(content: Data(reason.utf8), contentContext: context, completion: .contentProcessed { error in
      if let error { Issue.record("sending the cable close failed: \(error)") }
    })
  }

  private func send(_ text: String, on connection: NWConnection) {
    let context = NWConnection.ContentContext(identifier: "frame", metadata: [NWProtocolWebSocket.Metadata(opcode: .text)])
    connection.send(content: Data(text.utf8), contentContext: context, completion: .contentProcessed { error in
      if let error { Issue.record("sending GraphQL frame failed: \(error)") }
    })
  }

}

private final class HelloCredentials: CredentialStore, @unchecked Sendable {
  private let lock = NSLock()
  private var value: String?
  func get() throws -> String? { lock.withLock { value } }
  func set(credential: String) throws { lock.withLock { value = credential } }
  func clear() throws { lock.withLock { value = nil } }
}

extension BridgeProbeTests {
  @MainActor
  @Test(.timeLimit(.minutes(1)))
  func encryptedHelloCloseReportsOneSessionRefusal() async throws {
    let http = try SealedServer { path, body in
      if path == "/api/auth/login_link" { return ["sessionId": body["account"] as? String ?? ""] }
      return ["error": "UNAUTHENTICATED"]
    }
    let peer = try GraphQLPeer(script: .refuseHello(http))
    defer { peer.stop() }
    let socket = try await peer.start()
    let origin = "http://127.0.0.1:\(try #require(socket.port))"
    let encrypted = EncryptedConfig(
      origin: origin, serverKey: SealedServer.serverKey, pin: SealedServer.pin,
      endpoints: EncryptedEndpoints(graphql: "/api/graphql", cable: "/api/cable", logout: "/api/logout", bootstrap: "/api/bootstrap", sessionOperations: [
        "login_link": EncryptedSessionOperation(path: "/api/auth/login_link", class: .authenticated),
        "probe": EncryptedSessionOperation(path: "/api/probe", class: .authenticated)
      ]),
      protocolLabel: "example-ws-v1", cablePurpose: "ws.hello", cableChannel: "EncryptedGraphqlChannel"
    )
    try await DB.openTesting(transport: UrlSessionTransport(), modelsJSON: modelsJSON, encrypted: encrypted, credentials: HelloCredentials(), http: http)
    let refusals = Cursor(try DB.sessionRefusals())
    do {
      let operations = #"[{"name":"BulletinAdded","document":"subscription BulletinAdded { bulletinAdded { body occurredAt } }","rootPath":"bulletinAdded"}]"#
      try http.registerDocuments(operations)
      try await registerStandOperations(DB.database, defsJson: operations)
      let frames = Cursor(peer.received.stream)
      var previousEpoch: UInt64?
      for account in ["first", "second"] {
        if previousEpoch != nil { try await DB.sessionReset() }
        try await DB.sessionCall("login_link", body: ["account": account])
        let epoch = try DB.sessionStatus().epoch
        #expect(epoch != previousEpoch)
        previousEpoch = epoch
        let closes = Cursor(try DB.cableCloses())
        guard case .ready? = await closes.next("the cable-close subscription of \(account)") else {
          Issue.record("the cable-close subscription did not become ready")
          break
        }
        let reading = Cursor(try DB.account().stream(Notice.recent))
        defer { withExtendedLifetime(reading) {} }
        _ = try #require(await reading.next("the first notice snapshot of \(account)"))
        _ = try #require(await frames.next("the cable subscription of \(account)"))
        _ = try #require(await frames.next("the encrypted hello of \(account)"))
        guard case let .close(_, close)? = await closes.next("the cable close of \(account)") else {
          Issue.record("the cable did not report a close")
          break
        }
        #expect(close.code == 4401)
        #expect(close.reason == "session_unknown")
        let refusal = try #require(await refusals.next("the hello refusal of \(account)"))
        #expect(refusal.epoch == epoch)
        #expect(refusal.reason == .sessionEnded)
        #expect(http.pathRequests("/api/graphql") == 0)
      }

      do {
        try await DB.sessionCall("probe", body: [String: String]())
        Issue.record("the probe succeeded")
      } catch DbError.Unauthorized {}
      #expect(await refusals.next("the following HTTP refusal")?.reason == .unauthenticated)
    } catch {
      await DB.closeForCleanup()
      throw error
    }
    await DB.closeForCleanup()
  }
}

struct GraphQLSocketTests {
  @Test(.timeLimit(.minutes(1)), arguments: [false, true])
  func cableFramesUseThePublicTransport(remoteClose: Bool) async throws {
    let server = try GraphQLPeer(script: remoteClose ? .cableClose : .cable)
    defer { server.stop() }
    let origin = try await server.start()
    let session = URLSession(configuration: .ephemeral)
    defer { session.invalidateAndCancel() }
    let transport = UrlSessionTransport(session: session)
    let events = RecordingEvents()
    guard case let .open(connection) = await transport.openCable(url: cableURL(origin), headers: [:], connectMs: 10_000, events: events) else {
      Issue.record("the cable did not open")
      return
    }
    #expect(connection.send(text: "client frame"))
    let received = Cursor(server.received.stream)
    #expect(await received.next("client frame", timeout: .seconds(2)) == "client frame")
    #expect(await events.next() == .frame("server frame"))
    if remoteClose {
      guard case let .ended(code, reason, _)? = await events.next() else { Issue.record("the cable did not end"); return }
      #expect((code, reason) == (4401, "UNAUTHENTICATED"))
    }
    connection.close(code: 1000, reason: "")
  }
}
