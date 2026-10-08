import Foundation

public enum TransportIdentity {
  public static func resolve(operationName: String, token: String?, deviceUUID: String) throws -> String {
    if PublicOperations.names.contains(operationName) {
      return deviceUUID
    }
    guard let token, !token.isEmpty else {
      throw GraphQLTransportError.unauthenticated
    }
    return token
  }

  public static func authorizationToken(operationName: String, token: String?) -> String? {
    if PublicOperations.names.contains(operationName) {
      return nil
    }
    return token
  }
}
