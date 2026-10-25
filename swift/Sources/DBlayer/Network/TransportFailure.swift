import Foundation

enum TransportFailure {
  static func classifySubscription(_ error: Error) -> DbError { classify(error, kind: .query) }

  static func ofStoredTask(detail: String) -> DbError { .Network(detail: detail) }

  static func classifySocket(_ error: Error) -> DbError {
    if let error = error as? DbError { return error }
    return classify(error, kind: .query)
  }

  static func classifyEncryptedHttp(_ error: Error) -> DbError {
    if let error = error as? DbError { return error }
    if error is CancellationError { return .Interrupted(detail: error.localizedDescription) }
    if let error = error as? URLError {
      if error.code == .cancelled { return .Interrupted(detail: error.localizedDescription) }
      if [.secureConnectionFailed, .serverCertificateHasBadDate, .serverCertificateUntrusted, .serverCertificateHasUnknownRoot, .serverCertificateNotYetValid, .clientCertificateRejected, .clientCertificateRequired].contains(error.code) {
        return .Security(code: .tlsTrust, detail: error.localizedDescription)
      }
    }
    return .Network(detail: error.localizedDescription)
  }

  private static let notDelivered: Set<URLError.Code> = [
    .notConnectedToInternet, .cannotFindHost, .cannotConnectToHost, .dnsLookupFailed, .dataNotAllowed, .internationalRoamingOff
  ]

  static func classify(_ error: Error, kind: GraphQLOperationKind) -> DbError {
    if let error = error as? DbError { return error }
    let detail = String(describing: error)
    switch error {
    case is CancellationError: return .Interrupted(detail: detail)
    case let failure as URLError:
      if failure.code == .cancelled { return .Interrupted(detail: failure.localizedDescription) }
      return notDelivered.contains(failure.code) || kind == .query ? .Network(detail: failure.localizedDescription) : .Interrupted(detail: failure.localizedDescription)
    default: return kind == .query ? .Network(detail: detail) : .Interrupted(detail: detail)
    }
  }

}
