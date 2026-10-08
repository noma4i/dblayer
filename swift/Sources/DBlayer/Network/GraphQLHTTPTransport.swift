import Foundation

public struct GraphQLHTTPTransport: Sendable {
  private let url: URL
  private let session: URLSession
  private let signer: RequestSigner
  private let clock: ServerClock
  private let appVersion: String
  private let appBuild: String
  private let deviceUUID: String
  private let token: String?

  public init(
    url: URL,
    session: URLSession,
    signer: RequestSigner,
    clock: ServerClock,
    appVersion: String,
    appBuild: String,
    deviceUUID: String,
    token: String?
  ) {
    self.url = url
    self.session = session
    self.signer = signer
    self.clock = clock
    self.appVersion = appVersion
    self.appBuild = appBuild
    self.deviceUUID = deviceUUID
    self.token = token
  }

  public static func makeSession(configuration: URLSessionConfiguration = .default) -> URLSession {
    configuration.timeoutIntervalForRequest = 30
    configuration.timeoutIntervalForResource = 60
    return URLSession(configuration: configuration)
  }

  public func send(
    operationName: String,
    documentHash: String,
    document: String?,
    variablesJSON: Data
  ) async throws -> (status: Int, body: Data) {
    let identity = try TransportIdentity.resolve(operationName: operationName, token: token, deviceUUID: deviceUUID)
    let now = await clock.now()
    let nonce = RequestSigner.makeNonce()
    let signed = signer.sign(
      SignatureInput(
        operationName: operationName,
        variablesJSON: variablesJSON,
        identity: identity,
        deviceUUID: deviceUUID,
        now: now,
        nonce: nonce
      )
    )
    guard let requestBody = GraphQLRequestBody.make(
      operationName: operationName,
      documentHash: documentHash,
      variablesJSON: variablesJSON,
      document: document
    ) else {
      throw GraphQLTransportError.server(status: 0)
    }
    let encryptedRequestBody = try JSONEncoder().encode(signer.payloadCipher().seal(requestBody))

    let headers = TransportHeaders.make(
      appVersion: appVersion,
      appBuild: appBuild,
      deviceUUID: deviceUUID,
      token: TransportIdentity.authorizationToken(operationName: operationName, token: token),
      signed: signed
    )
    let request = GraphQLURLRequest.make(url: url, body: encryptedRequestBody, headers: headers)
    let (body, response) = try await session.data(for: request)
    guard let httpResponse = response as? HTTPURLResponse else {
      throw GraphQLTransportError.server(status: 0)
    }

    await clock.update(
      fromDateHeader: httpResponse.value(forHTTPHeaderField: "Date"),
      receivedAt: Date()
    )
    let responseBody = try responseBody(body, from: httpResponse)
    return (status: httpResponse.statusCode, body: responseBody)
  }

  private func responseBody(_ body: Data, from response: HTTPURLResponse) throws -> Data {
    guard response.value(forHTTPHeaderField: "x-payload-encryption") == "1" else {
      return body
    }

    do {
      let responseEnvelope = try JSONDecoder().decode(PayloadEnvelope.self, from: body)
      return try signer.payloadCipher().open(responseEnvelope)
    } catch {
      throw PayloadTransportError.responseDecryptionFailed(status: response.statusCode)
    }
  }
}
