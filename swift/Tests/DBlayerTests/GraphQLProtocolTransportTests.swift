@testable import DBlayer
import Foundation
import Testing

private struct PingQuery: GraphQLQuery {
  struct Data: Decodable, Sendable {
    let ping: String
  }

  struct Variables: Encodable, Sendable {}

  static let document = "query Ping { ping }"
  static let operationName = "Ping"
}

private final class CancellationCounter: @unchecked Sendable {
  var count = 0
}

private enum TestTransportError: Error {
  case unsupported
}

private struct TestTransport: GraphQLTransport {
  let response: Response<PingQuery.Data>

  func query<Operation: GraphQLQuery>(
    _ operation: Operation.Type,
    variables: Operation.Variables
  ) async throws -> Response<Operation.Data> {
    guard let data = response.data as? Operation.Data else {
      throw TestTransportError.unsupported
    }

    return Response(data: data, errors: response.errors)
  }

  func mutate<Operation: GraphQLMutation>(
    _ operation: Operation.Type,
    variables: Operation.Variables
  ) async throws -> Response<Operation.Data> {
    throw TestTransportError.unsupported
  }

  func subscribe<Operation: GraphQLSubscription>(
    _ operation: Operation.Type,
    variables: Operation.Variables,
    onReady: @escaping @Sendable () -> Void,
    onNext: @escaping @Sendable (Response<Operation.Data>) -> Void,
    onError: @escaping @Sendable (SubscriptionFailure) -> Void
  ) -> SubscriptionHandle {
    SubscriptionHandle(onCancel: {})
  }
}

@Suite(.serialized)
struct GraphQLTransportTests {
  @Test
  func cancelCallsOnCancelOnce() {
    let counter = CancellationCounter()
    let handle = SubscriptionHandle { counter.count += 1 }

    handle.cancel()
    handle.cancel()

    #expect(counter.count == 1)
  }
}
