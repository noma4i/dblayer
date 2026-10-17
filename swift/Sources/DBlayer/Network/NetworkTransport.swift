
import Foundation

public struct NetworkTransport: GraphQLTransport {
  private let run: OperationRun
  private let cable: CableClient

  public init(run: OperationRun, cable: CableClient) {
    self.run = run
    self.cable = cable
  }

  public func query<Operation: GraphQLQuery>(
    _ operation: Operation.Type,
    variables: Operation.Variables
  ) async throws -> Response<Operation.Data> {
    let body = try await run.send(
      operationName: Operation.operationName,
      document: Operation.document,
      variables: variables,
      kind: .query
    )
    return try decodeResponse(body, as: Operation.Data.self)
  }

  public func mutate<Operation: GraphQLMutation>(
    _ operation: Operation.Type,
    variables: Operation.Variables
  ) async throws -> Response<Operation.Data> {
    let body = try await run.send(
      operationName: Operation.operationName,
      document: Operation.document,
      variables: variables,
      kind: .mutation,
      dataSystemMutation: true
    )
    return try decodeResponse(body, as: Operation.Data.self)
  }

  public func subscribe<Operation: GraphQLSubscription>(
    _ operation: Operation.Type,
    variables _: Operation.Variables,
    onReady: @escaping @Sendable () -> Void,
    onNext: @escaping @Sendable (Response<Operation.Data>) -> Void,
    onError: @escaping @Sendable (SubscriptionFailure) -> Void
  ) -> SubscriptionHandle {
    let task = Task { [cable] in
      await runSubscription(
        cable: cable,
        operation: operation,
        onReady: onReady,
        onNext: onNext,
        onError: onError
      )
    }

    return SubscriptionHandle {
      task.cancel()
    }
  }
}

private struct NetworkResponseBody<Decoded: Decodable>: Decodable {
  let data: Decoded?
  let errors: [GraphQLError]?
}

private func decodeResponse<Decoded: Decodable & Sendable>(
  _ body: Data,
  as type: Decoded.Type
) throws -> Response<Decoded> {
  let payload = try JSONDecoder().decode(NetworkResponseBody<Decoded>.self, from: body)
  return Response(data: payload.data, errors: payload.errors ?? [])
}

private func runSubscription<Operation: GraphQLSubscription>(
  cable: CableClient,
  operation: Operation.Type,
  onReady: @escaping @Sendable () -> Void,
  onNext: @escaping @Sendable (Response<Operation.Data>) -> Void,
  onError: @escaping @Sendable (SubscriptionFailure) -> Void
) async {
  do {
    let stream = await cable.subscribeGraphQL(
      document: operation.document,
      operationName: operation.operationName
    )
    for try await event in stream {
      try Task.checkCancellation()
      try deliverSubscriptionEvent(
        event,
        operation: operation,
        onReady: onReady,
        onNext: onNext,
        onError: onError
      )
    }
    guard !Task.isCancelled else { return }
    onError(SubscriptionFailure(error: TransportFailure.subscriptionCompleted))
  } catch is CancellationError {
    return
  } catch {
    guard !Task.isCancelled else { return }
    onError(
      SubscriptionFailure(
        error: TransportFailure.classifySubscription(error)
      )
    )
  }
}

private func deliverSubscriptionEvent<Operation: GraphQLSubscription>(
  _ event: GraphQLSubscriptionEvent,
  operation: Operation.Type,
  onReady: @escaping @Sendable () -> Void,
  onNext: @escaping @Sendable (Response<Operation.Data>) -> Void,
  onError: @escaping @Sendable (SubscriptionFailure) -> Void
) throws {
  switch event {
  case .ready:
    onReady()
  case let .payload(value):
    switch decodeSubscriptionPayload(value, as: Operation.Data.self) {
    case let .success(data):
      try Task.checkCancellation()
      onNext(Response(data: data))
    case let .failure(error):
      onError(
        SubscriptionFailure(
          error: .Invalid(detail: String(describing: error))
        )
      )
    }
  case .completed:
    break
  case let .errors(error):
    onError(SubscriptionFailure(error: error))
  }
}

private func decodeSubscriptionPayload<Decoded: Decodable>(
  _ value: JSONValue,
  as type: Decoded.Type
) -> Result<Decoded, any Error> {
  Result {
    try JSONDecoder().decode(type, from: JSONEncoder().encode(value))
  }
}
