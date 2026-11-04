import Foundation
import Network
import Testing
@testable import DBlayer

private final class GraphQLSignals: SubscriptionSink, @unchecked Sendable {
  let events = AsyncStream<String>.makeStream()
  private let lock = NSLock()
  private var openedToken: String?
  var token: String? { lock.withLock { openedToken } }
  override func opened(token: String) throws { lock.withLock { openedToken = token } }
  override func ready() { events.continuation.yield("ready") }
  override func event(json: String) throws { events.continuation.yield(json) }
  override func failed(error: DbError) {}
  override func closed() { events.continuation.yield("closed") }
}

private final class GraphQLPeer: @unchecked Sendable {
  enum Script { case close, malformed, frames, cable, cableClose }
  private let listener: NWListener
  private let script: Script
  private let lock = NSLock()
  private var connection: NWConnection?
  let received = AsyncStream<String>.makeStream()

  init(script: Script) throws {
    self.script = script
    let subprotocol: String?
    switch script {
    case .cable, .cableClose: subprotocol = nil
    default: subprotocol = "graphql-transport-ws"
    }
    let socket = NWProtocolWebSocket.Options()
    socket.setClientRequestHandler(.global()) { _, _ in
      NWProtocolWebSocket.Response(status: .accept, subprotocol: subprotocol)
    }
    let parameters = NWParameters.tcp
    parameters.defaultProtocolStack.applicationProtocols.insert(socket, at: 0)
    listener = try NWListener(using: parameters, on: .any)
  }

  func start() async throws -> URL {
    listener.newConnectionHandler = { [self] connection in
      lock.withLock { self.connection = connection }
      connection.start(queue: .global())
      receive(connection)
    }
    listener.start(queue: .global())
    await eventually("GraphQL WebSocket listener ready") { self.listener.state == .ready }
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
      switch script {
      case .cable, .cableClose:
        send("server frame", on: connection)
        if case .cableClose = script {
          let metadata = NWProtocolWebSocket.Metadata(opcode: .close)
          metadata.closeCode = .privateCode(4401)
          let context = NWConnection.ContentContext(identifier: "close", metadata: [metadata])
          connection.send(content: Data("UNAUTHENTICATED".utf8), contentContext: context, completion: .contentProcessed { _ in })
        } else {
          receive(connection)
        }
        return
      default: break
      }
      do {
        let object = try JSONSerialization.jsonObject(with: data) as? [String: Any]
        switch object?["type"] as? String {
        case "connection_init":
          let context = NWConnection.ContentContext(identifier: "ack", metadata: [NWProtocolWebSocket.Metadata(opcode: .text)])
          connection.send(content: Data(#"{"type":"connection_ack"}"#.utf8), contentContext: context, completion: .contentProcessed { _ in })
          receive(connection)
        case "subscribe":
          switch script {
          case .close:
            let metadata = NWProtocolWebSocket.Metadata(opcode: .close)
            metadata.closeCode = .protocolCode(.normalClosure)
            let context = NWConnection.ContentContext(identifier: "close", metadata: [metadata])
            connection.send(content: Data(), contentContext: context, completion: .contentProcessed { _ in })
          case .malformed:
            send("{", on: connection)
          case .frames:
            for frame in [#"{"type":"ignored"}"#, "[]", #"{"type":"next"}"#, #"{"type":"next","payload":{"data":{"id":"7"}}}"#, #"{"type":"error","payload":[{"message":"denied"}]}"#, #"{"type":"ping"}"#] {
              send(frame, on: connection)
            }
            receive(connection)
          case .cable, .cableClose: break
          }
        case "pong":
          send(#"{"type":"complete"}"#, on: connection)
        default:
          Issue.record("unexpected GraphQL frame")
        }
      } catch { Issue.record("invalid GraphQL frame: \(error)") }
    }
  }

  private func send(_ text: String, on connection: NWConnection) {
    let context = NWConnection.ContentContext(identifier: "frame", metadata: [NWProtocolWebSocket.Metadata(opcode: .text)])
    connection.send(content: Data(text.utf8), contentContext: context, completion: .contentProcessed { error in
      if let error { Issue.record("sending GraphQL frame failed: \(error)") }
    })
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
    let transport = UrlSessionTransport(endpoint: origin, session: session, cablePath: "/api/cable")
    let lease = RecordingLease(noHandle: .init())
    try await transport.openCable(lease: lease)
    let token = try #require(lease.token)
    try await transport.cableSend(token: token, text: "client frame")
    let received = Cursor(server.received.stream)
    #expect(await received.next("client frame", timeout: .seconds(2)) == "client frame")
    #expect(await transport.cableNext(token: token) == .text(text: "server frame"))
    if remoteClose {
      #expect(await transport.cableNext(token: token) == .closed(code: 4401, reason: "UNAUTHENTICATED"))
    }
    await transport.cableClose(token: token)
    #expect(await transport.cableNext(token: token) == nil)
  }

  @Test(.timeLimit(.minutes(1)), arguments: [false, true])
  func terminalFramesEndTheGraphQLSubscription(malformed: Bool) async throws {
    let server = try GraphQLPeer(script: malformed ? .malformed : .close)
    defer { server.stop() }
    let origin = try await server.start()
    let session = URLSession(configuration: .ephemeral)
    defer { session.invalidateAndCancel() }
    let transport = UrlSessionTransport(endpoint: origin, session: session)
    let sink = GraphQLSignals(noHandle: .init())
    let events = Cursor(sink.events.stream)
    try await transport.subscribe(request: TransportRequest(document: "subscription Probe { id }", operationName: "Probe", variablesJson: "{}", purpose: .standard), sink: sink)
    defer { if let token = sink.token { transport.unsubscribe(token: token) } }
    #expect(await events.next("GraphQL ready", timeout: .seconds(2)) == "ready")
    #expect(await events.next("remote close", timeout: .seconds(2)) == "closed")
  }

  @Test(.timeLimit(.minutes(1)))
  func framesDeliverPayloadsAnswerPingAndComplete() async throws {
    let server = try GraphQLPeer(script: .frames)
    defer { server.stop() }
    let origin = try await server.start()
    let session = URLSession(configuration: .ephemeral)
    defer { session.invalidateAndCancel() }
    let transport = UrlSessionTransport(endpoint: origin, headers: { ["Authorization": "Bearer probe"] }, session: session)
    let sink = GraphQLSignals(noHandle: .init())
    let events = Cursor(sink.events.stream)
    let received = Cursor(server.received.stream)
    try await transport.subscribe(request: TransportRequest(document: "subscription Probe($id: ID!) { node(id: $id) { id } }", operationName: "Probe", variablesJson: #"{"id":"7"}"#, purpose: .standard), sink: sink)
    defer { if let token = sink.token { transport.unsubscribe(token: token) } }
    let initial = try JSONSerialization.jsonObject(with: Data(try #require(await received.next("connection init", timeout: .seconds(2))).utf8)) as? [String: Any]
    #expect((initial?["payload"] as? [String: String])?["Authorization"] == "Bearer probe")
    let subscription = try JSONSerialization.jsonObject(with: Data(try #require(await received.next("subscription", timeout: .seconds(2))).utf8)) as? [String: Any]
    let payload = subscription?["payload"] as? [String: Any]
    #expect(subscription?["id"] as? String == sink.token)
    #expect(payload?["operationName"] as? String == "Probe")
    #expect((payload?["variables"] as? [String: String])?["id"] == "7")
    #expect(await events.next("ready", timeout: .seconds(2)) == "ready")
    let next = try JSONSerialization.jsonObject(with: Data(try #require(await events.next("next payload", timeout: .seconds(2))).utf8)) as? [String: Any]
    #expect((next?["data"] as? [String: String])?["id"] == "7")
    let failure = try JSONSerialization.jsonObject(with: Data(try #require(await events.next("error payload", timeout: .seconds(2))).utf8)) as? [String: Any]
    #expect((failure?["errors"] as? [[String: String]])?.first?["message"] == "denied")
    let pong = try JSONSerialization.jsonObject(with: Data(try #require(await received.next("pong", timeout: .seconds(2))).utf8)) as? [String: String]
    #expect(pong?["type"] == "pong")
    #expect(await events.next("complete", timeout: .seconds(2)) == "closed")
  }
}
