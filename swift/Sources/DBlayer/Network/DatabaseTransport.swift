import Foundation

/// Serves the database core over the signed HTTP pipeline (`OperationRun`) and the Cable socket.
public final class DatabaseTransport: Transport, @unchecked Sendable {
  private let run: @Sendable () async throws -> OperationRun
  private let cable: @Sendable () async -> CableClient?
  private let onEvent: @Sendable (String, String) -> Void
  private let uploads = UrlSessionTransport(endpoint: URL(fileURLWithPath: "/"))
  private let lock = NSLock()
  private var tasks: [String: Task<Void, Never>] = [:]

  public init(
    run: @escaping @Sendable () async throws -> OperationRun,
    cable: @escaping @Sendable () async -> CableClient?,
    onEvent: @escaping @Sendable (String, String) -> Void = { _, _ in }
  ) {
    self.run = run
    self.cable = cable
    self.onEvent = onEvent
  }

  public func query(request: TransportRequest) async throws -> String { try await send(request, kind: .query) }
  public func mutate(request: TransportRequest) async throws -> String { try await send(request, kind: .mutation) }

  public func subscribe(request: TransportRequest, sink: SubscriptionSink) async throws -> String {
    guard let client = await cable() else { throw DbError.Network(detail: "the subscription socket is not open") }
    let token = UUID().uuidString
    let onEvent = onEvent
    let task = Task {
      await Self.relay(
        events: await client.subscribeGraphQL(document: request.document, operationName: request.operationName),
        operationName: request.operationName,
        sink: sink,
        onEvent: onEvent
      )
    }
    lock.withLock { tasks[token] = task }
    return token
  }

  static func relay(
    events: AsyncThrowingStream<GraphQLSubscriptionEvent, Error>,
    operationName: String,
    sink: any SubscriptionSinkProtocol,
    onEvent: @Sendable (String, String) -> Void
  ) async {
    do {
      for try await event in events {
        switch event {
        case .ready: sink.ready()
        case let .payload(data):
          onEvent(operationName, data.text)
          try sink.event(json: JSONValue.object(["data": data]).text)
        case let .errors(messages): sink.failed(kind: "graphql", detail: messages.joined(separator: "\n"))
        case .completed: break
        }
      }
      if !Task.isCancelled { sink.failed(kind: "transport", detail: "subscription completed") }
    } catch is CancellationError {
    } catch {
      if !Task.isCancelled { sink.failed(kind: error as? CableClientError == .rejected ? "graphql" : "transport", detail: String(describing: error)) }
    }
  }

  public func unsubscribe(token: String) {
    lock.withLock { tasks.removeValue(forKey: token) }?.cancel()
  }

  public func put(url: String, headers: [String: String], bytes: Data, progress: ByteProgress) async throws -> PutReply {
    try await uploads.put(url: url, headers: headers, bytes: bytes, progress: progress)
  }

  private func send(_ request: TransportRequest, kind: GraphQLOperationKind) async throws -> String {
    do {
      let data = try await run().send(operationName: request.operationName, document: request.document, variables: JSONValue.parse(request.variablesJson), kind: kind)
      return String(decoding: data, as: UTF8.self)
    } catch {
      throw TransportFailure.classify(error, kind: kind)
    }
  }
}
