import Foundation

enum TransportFailure {
  private static let notDelivered: Set<URLError.Code> = [
    .notConnectedToInternet, .cannotFindHost, .cannotConnectToHost, .dnsLookupFailed, .dataNotAllowed, .internationalRoamingOff
  ]

  static func classify(_ error: Error, kind: GraphQLOperationKind) -> DbError {
    let detail = String(describing: error)
    switch error {
    case let failure as GraphQLTransportError: return answered(failure, detail: detail)
    case is CancellationError: return .Interrupted(detail: detail)
    case let failure as URLError:
      if failure.code == .cancelled { return .Interrupted(detail: failure.localizedDescription) }
      return notDelivered.contains(failure.code) || kind == .query ? .Network(detail: failure.localizedDescription) : .Interrupted(detail: failure.localizedDescription)
    default: return kind == .query ? .Network(detail: detail) : .Interrupted(detail: detail)
    }
  }

  private static func answered(_ failure: GraphQLTransportError, detail: String) -> DbError {
    switch failure {
    case .unauthenticated: .Http(status: 401, detail: detail)
    case let .payloadTransport(_, status), let .server(status): .Http(status: UInt16(clamping: status >= 400 ? status : 500), detail: detail)
    case let .graphQL(messages, extensions): .Graphql(messages: messages, extensions: extensions)
    }
  }
}
