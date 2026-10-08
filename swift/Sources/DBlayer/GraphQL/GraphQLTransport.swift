public protocol GraphQLTransport: Sendable {
  func query<Operation: GraphQLQuery>(
    _ operation: Operation.Type,
    variables: Operation.Variables
  ) async throws -> Response<Operation.Data>

  func mutate<Operation: GraphQLMutation>(
    _ operation: Operation.Type,
    variables: Operation.Variables
  ) async throws -> Response<Operation.Data>

  func subscribe<Operation: GraphQLSubscription>(
    _ operation: Operation.Type,
    variables: Operation.Variables,
    onReady: @escaping @Sendable () -> Void,
    onNext: @escaping @Sendable (Response<Operation.Data>) -> Void,
    onError: @escaping @Sendable (SubscriptionFailure) -> Void
  ) -> SubscriptionHandle
}
