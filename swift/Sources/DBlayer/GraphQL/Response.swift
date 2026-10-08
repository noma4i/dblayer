public struct Response<Data: Sendable>: Sendable {
  public let data: Data?
  public let errors: [GraphQLError]

  public init(data: Data?, errors: [GraphQLError] = []) {
    self.data = data
    self.errors = errors
  }
}
