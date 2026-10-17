
import Foundation

public struct OperationRun: Sendable {
  private let transport: GraphQLHTTPTransport
  private let hashes: PersistedQueryHashes
  private let onFailure: @Sendable (RequestFailure) -> Void

  public init(
    transport: GraphQLHTTPTransport,
    hashes: PersistedQueryHashes,
    onFailure: @escaping @Sendable (RequestFailure) -> Void = { _ in }
  ) {
    self.transport = transport
    self.hashes = hashes
    self.onFailure = onFailure
  }

  public func send(
    operationName: String,
    document: String,
    variables: some Encodable,
    kind: GraphQLOperationKind,
    dataSystemMutation: Bool = false
  ) async throws -> Data {
    let variablesJSON = try canonicalVariablesJSON(variables)
    let documentHash = await hashes.hash(for: document)
    let send = { (document: String?) async throws in
      try await post(
        operationName: operationName,
        documentHash: documentHash,
        document: document,
        variablesJSON: variablesJSON
      )
    }
    var result = try await send(nil)
    var retryPolicy: TransportRetryPolicy? = TransportRetryPolicy()

    while true {
      let response = GraphQLResponseBody.parse(result.body)
      let retryReason = response?.errors.first.flatMap {
        [200, 403].contains(result.status)
          ? TransportRetryPolicy.reason(forErrorMessage: $0.message, code: $0.code)
          : nil
      }
      if let retryReason, retryPolicy?.shouldRetry(reason: retryReason, kind: kind) == true {
        retryPolicy = nil
        result = try await send(retryReason == .persistedQueryNotFound ? document : nil)
        continue
      }
      let returnsMutationBody = kind == .mutation && (dataSystemMutation || response?.data != nil)
      if (200 ... 299).contains(result.status), returnsMutationBody {
        if response?.errors.isEmpty == false {
          onFailure(RequestFailure(operationName: operationName, status: result.status, errorClass: "graphQL"))
        }
        return result.body
      }
      if let error = GraphQLTransportError.classify(
        status: result.status,
        errorMessages: response?.errors.map(\.message) ?? [],
        extensions: response?.errors.isEmpty == false ? try graphqlExtensions(responseJson: String(decoding: result.body, as: UTF8.self)) : nil
      ) {
        onFailure(RequestFailure(operationName: operationName, status: result.status, errorClass: error.errorClass))
        throw error
      }
      return result.body
    }
  }
}

public extension OperationRun {
  private func post(
    operationName: String,
    documentHash: String,
    document: String?,
    variablesJSON: Data
  ) async throws -> (status: Int, body: Data) {
    do {
      return try await transport.send(
        operationName: operationName,
        documentHash: documentHash,
        document: document,
        variablesJSON: variablesJSON
      )
    } catch {
      onFailure(RequestFailure(operationName: operationName, status: nil, errorClass: "\(type(of: error))"))
      throw error
    }
  }
}

private func canonicalVariablesJSON(_ variables: some Encodable) throws -> Data {
  let encodedVariables = try JSONEncoder().encode(variables)
  let variablesValue = try JSONValue.decode(encodedVariables)
  return CanonicalJSON.encode(variablesValue)
}
