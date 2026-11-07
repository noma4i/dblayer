import Foundation
import Network
import Testing
@testable import DBlayer

private final class GraphQLPeer: @unchecked Sendable {
  enum Script { case cable, cableClose }
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
      send("server frame", on: connection)
      if case .cableClose = script {
        let metadata = NWProtocolWebSocket.Metadata(opcode: .close)
        metadata.closeCode = .privateCode(4401)
        let context = NWConnection.ContentContext(identifier: "close", metadata: [metadata])
        connection.send(content: Data("UNAUTHENTICATED".utf8), contentContext: context, completion: .contentProcessed { _ in })
      } else {
        receive(connection)
      }
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
    let transport = UrlSessionTransport(endpoint: origin, session: session)
    let events = RecordingEvents()
    guard case let .open(connection) = await transport.openCable(url: cableURL(origin), connectMs: 10_000, events: events) else {
      Issue.record("the cable did not open")
      return
    }
    #expect(connection.send(text: "client frame"))
    let received = Cursor(server.received.stream)
    #expect(await received.next("client frame", timeout: .seconds(2)) == "client frame")
    #expect(await events.next() == .frame("server frame"))
    if remoteClose {
      #expect(await events.next() == .closed(4401, "UNAUTHENTICATED"))
    }
    connection.close(code: 1000, reason: "")
  }
}
