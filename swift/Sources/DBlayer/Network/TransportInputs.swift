import Foundation

public struct TransportInputs: Sendable {
  public let appVersion: String
  public let appBuild: String
  public let deviceUUID: String
  public let tokenSupplier: @Sendable () -> String?

  public init(
    appVersion: String,
    appBuild: String,
    deviceUUID: String,
    tokenSupplier: @escaping @Sendable () -> String?
  ) {
    self.appVersion = appVersion
    self.appBuild = appBuild
    self.deviceUUID = deviceUUID
    self.tokenSupplier = tokenSupplier
  }
}

public extension GraphQLHTTPTransport {
  static func make(
    url: URL,
    inputs: TransportInputs,
    session: URLSession,
    signer: RequestSigner,
    clock: ServerClock
  ) -> GraphQLHTTPTransport {
    GraphQLHTTPTransport(
      url: url,
      session: session,
      signer: signer,
      clock: clock,
      appVersion: inputs.appVersion,
      appBuild: inputs.appBuild,
      deviceUUID: inputs.deviceUUID,
      tokenSupplier: inputs.tokenSupplier
    )
  }
}
