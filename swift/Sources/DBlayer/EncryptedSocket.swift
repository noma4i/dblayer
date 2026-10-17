extension Transport {
  public func openCable() async throws -> String {
    throw DbError.Invalid(detail: "encrypted cable socket is not configured")
  }
  public func cableSend(token: String, text: String) async throws {
    throw DbError.Invalid(detail: "encrypted cable socket is not configured")
  }
  public func cableNext(token: String) async -> EncryptedSocketEvent? { nil }
  public func cableClose(token: String) async {}
}
