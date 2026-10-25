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
  override func event(json: String) throws {}
  override func failed(error: DbError) {}
  override func closed() { events.continuation.yield("closed") }
}

private final class GraphQLPeer: @unchecked Sendable {
  private let listener: NWListener
  private let malformed: Bool
  private let lock = NSLock()
  private var connection: NWConnection?

  init(malformed: Bool) throws {
    self.malformed = malformed
    let socket = NWProtocolWebSocket.Options()
    socket.setClientRequestHandler(.global()) { _, _ in
      NWProtocolWebSocket.Response(status: .accept, subprotocol: "graphql-transport-ws")
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
    lock.withLock { connection }?.cancel()
  }

  private func receive(_ connection: NWConnection) {
    connection.receiveMessage { [self] data, _, _, error in
      guard error == nil, let data else { return }
      do {
        let object = try JSONSerialization.jsonObject(with: data) as? [String: Any]
        switch object?["type"] as? String {
        case "connection_init":
          let context = NWConnection.ContentContext(identifier: "ack", metadata: [NWProtocolWebSocket.Metadata(opcode: .text)])
          connection.send(content: Data(#"{"type":"connection_ack"}"#.utf8), contentContext: context, completion: .contentProcessed { _ in })
          receive(connection)
        case "subscribe":
          let metadata = NWProtocolWebSocket.Metadata(opcode: malformed ? .text : .close)
          if !malformed { metadata.closeCode = .protocolCode(.normalClosure) }
          let context = NWConnection.ContentContext(identifier: "close", metadata: [metadata])
          connection.send(content: malformed ? Data("{".utf8) : Data(), contentContext: context, completion: .contentProcessed { _ in })
        default:
          Issue.record("unexpected GraphQL frame")
        }
      } catch { Issue.record("invalid GraphQL frame: \(error)") }
    }
  }
}

struct GraphQLSocketTests {
  @Test(.timeLimit(.minutes(1)), arguments: [false, true])
  func terminalFramesEndTheGraphQLSubscription(malformed: Bool) async throws {
    let server = try GraphQLPeer(malformed: malformed)
    defer { server.stop() }
    let origin = try await server.start()
    let session = URLSession(configuration: .ephemeral)
    defer { session.invalidateAndCancel() }
    let transport = UrlSessionTransport(endpoint: origin, session: session)
    let sink = GraphQLSignals(noHandle: .init())
    let events = Cursor(sink.events.stream)
    try await transport.subscribe(request: TransportRequest(document: "subscription Probe { id }", operationName: "Probe", variablesJson: "{}"), sink: sink)
    defer { if let token = sink.token { transport.unsubscribe(token: token) } }
    #expect(await events.next("GraphQL ready", timeout: .seconds(2)) == "ready")
    #expect(await events.next("remote close", timeout: .seconds(2)) == "closed")
  }
}
