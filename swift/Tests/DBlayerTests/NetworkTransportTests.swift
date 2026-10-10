import DBlayerTesting
import DBlayer
import Foundation
import Testing
@testable import DBlayer

private struct ProbeQuery: GraphQLQuery {
  struct Data: Decodable, Sendable {
    let id: String
  }

  struct Variables: Encodable, Sendable {}

  static let document = "query Probe { probe { id } }"
  static let operationName = "Probe"
}

private struct ProbeMutation: GraphQLMutation {
  struct Data: Decodable, Sendable {
    let id: String
  }

  struct Variables: Encodable, Sendable {}

  static let document = "mutation Probe { probe { id } }"
  static let operationName = "Probe"
}

private struct ProbeSubscription: GraphQLSubscription {
  struct Data: Decodable, Sendable {
    let id: String
  }

  struct Variables: Encodable, Sendable {}

  static let document = "subscription Probe { probe { id } }"
  static let operationName = "Probe"
}

private enum SubscriptionCallback: Sendable, Equatable {
  case ready
  case payload(String)
  case error(SubscriptionFailure)
}

@Suite(.serialized)
struct NetworkTransportTests {
  @Test
  func queryDecodesDataAndNullData() async throws {
    let bodies = [#"{"data":{"id":"7"}}"#, #"{"data":null}"#]
    var ids: [String?] = []
    for (index, body) in bodies.enumerated() {
      let scriptId = "network-transport-query-\(index)"
      ScriptedURLProtocol.reset(scriptId: scriptId)
      defer { ScriptedURLProtocol.reset(scriptId: scriptId) }

      let (httpTransport, url) = try makeScriptedTransport(scriptId: scriptId)
      ScriptedURLProtocol.push(scriptId: scriptId, status: 200, headers: [:], body: Data(body.utf8), url: url)
      let run = OperationRun(transport: httpTransport, hashes: PersistedQueryHashes())
      let transport: any GraphQLTransport = NetworkTransport(run: run, cable: makeCableClient())

      let response = try await transport.query(ProbeQuery.self, variables: ProbeQuery.Variables())
      ids.append(response.data?.id)
    }

    #expect(ids == ["7", nil])
  }

  @Test(arguments: [200, 201, 400, 403, 422])
  func querySurfacesGraphQLErrorsFromEveryStatus(status: Int) async throws {
    let scriptId = "network-transport-error-status-\(status)"
    ScriptedURLProtocol.reset(scriptId: scriptId)
    defer { ScriptedURLProtocol.reset(scriptId: scriptId) }

    let (httpTransport, url) = try makeScriptedTransport(scriptId: scriptId)
    ScriptedURLProtocol.push(
      scriptId: scriptId,
      status: status,
      headers: [:],
      body: Data(#"{"errors":[{"message":"Rejected","extensions":{"code":"REGISTRATION_REQUIRED"}}]}"#.utf8),
      url: url
    )
    let run = OperationRun(transport: httpTransport, hashes: PersistedQueryHashes())
    let transport: any GraphQLTransport = NetworkTransport(run: run, cable: makeCableClient())

    await #expect(throws: GraphQLTransportError.graphQL(messages: ["Rejected"])) {
      _ = try await transport.query(ProbeQuery.self, variables: ProbeQuery.Variables())
    }
    #expect(ScriptedURLProtocol.recordedRequests(scriptId: scriptId).count == 1)
  }

  @Test(arguments: [(200, true), (200, false), (401, false)])
  func mutationReturnsDataAndErrors(status: Int, hasData: Bool) async throws {
    let scriptId = "network-mutation-partial-\(hasData)"
    ScriptedURLProtocol.reset(scriptId: scriptId)
    defer { ScriptedURLProtocol.reset(scriptId: scriptId) }
    let (httpTransport, url) = try makeScriptedTransport(scriptId: scriptId)
    let data = hasData ? #"{"id":"7"}"# : "null"
    let body = Data("{\"data\":\(data),\"errors\":[{\"message\":\"Rejected\",\"extensions\":{\"code\":\"BAD_INPUT\"}}]}".utf8)
    ScriptedURLProtocol.push(scriptId: scriptId, status: status, headers: [:], body: body, url: url)
    let run = OperationRun(transport: httpTransport, hashes: PersistedQueryHashes())
    let transport: any GraphQLTransport = NetworkTransport(run: run, cable: makeCableClient())

    if status == 401 {
      await #expect(throws: GraphQLTransportError.unauthenticated) {
        _ = try await transport.mutate(ProbeMutation.self, variables: ProbeMutation.Variables())
      }
      return
    }
    let response = try await transport.mutate(ProbeMutation.self, variables: ProbeMutation.Variables())

    #expect(response.data?.id == (hasData ? "7" : nil))
    #expect(response.errors.map(\.message) == ["Rejected"])
    #expect(response.errors.map(\.code) == ["BAD_INPUT"])
    #expect(ScriptedURLProtocol.recordedRequests(scriptId: scriptId).count == 1)
  }

  @Test(arguments: [true, false])
  func directMutationThrowsOnlyWithoutData(hasData: Bool) async throws {
    let scriptId = "direct-mutation-partial-\(hasData)"
    ScriptedURLProtocol.reset(scriptId: scriptId)
    defer { ScriptedURLProtocol.reset(scriptId: scriptId) }
    let (httpTransport, url) = try makeScriptedTransport(scriptId: scriptId)
    let data = hasData ? #"{"id":"7"}"# : "null"
    let body = Data("{\"data\":\(data),\"errors\":[{\"message\":\"Rejected\"}]}".utf8)
    ScriptedURLProtocol.push(scriptId: scriptId, status: 200, headers: [:], body: body, url: url)
    let run = OperationRun(transport: httpTransport, hashes: PersistedQueryHashes())

    if hasData {
      let result = try await run.send(
        operationName: ProbeMutation.operationName,
        document: ProbeMutation.document,
        variables: ProbeMutation.Variables(),
        kind: .mutation
      )
      #expect(result == body)
    } else {
      await #expect(throws: GraphQLTransportError.graphQL(messages: ["Rejected"])) {
        _ = try await run.send(
          operationName: ProbeMutation.operationName,
          document: ProbeMutation.document,
          variables: ProbeMutation.Variables(),
          kind: .mutation
        )
      }
    }
    #expect(ScriptedURLProtocol.recordedRequests(scriptId: scriptId).count == 1)
  }

  @Test
  func subscriptionDeliversReadyAndPayload() async throws {
    let identifier = GraphQLChannel.identifier(channelId: "abc")
    let readyPayload = #"{"result":{"data":null},"more":true,"requestId":"abc:1","subscriptionId":"s1","subscriptionReady":true}"#
    let payload = #"{"result":{"data":{"id":"9"}},"more":true,"subscriptionId":"s1"}"#
    let socket = TestCableClientSocket(
      frames: [
        "{\"type\":\"welcome\"}",
        confirmationFrame(identifier: identifier),
        messageFrame(identifier: identifier, payload: readyPayload),
        messageFrame(identifier: identifier, payload: payload),
        rejectionFrame(identifier: identifier)
      ]
    )
    let cable = makeCableClient(socket: socket)
    let (httpTransport, _) = try makeScriptedTransport(
      scriptId: "network-transport-subscription-ready"
    )
    let transport: any GraphQLTransport = NetworkTransport(
      run: OperationRun(transport: httpTransport, hashes: PersistedQueryHashes()),
      cable: cable
    )
    let (callbacks, continuation) = AsyncStream<SubscriptionCallback>.makeStream()
    let handle = transport.subscribe(
      ProbeSubscription.self,
      variables: ProbeSubscription.Variables(),
      onReady: { continuation.yield(.ready) },
      onNext: { response in continuation.yield(.payload(response.data?.id ?? "")) },
      onError: { failure in continuation.yield(.error(failure)) }
    )
    let startTask = Task { await cable.start() }
    var iterator = callbacks.makeAsyncIterator()

    let ready = await iterator.next()
    let receivedPayload = await iterator.next()

    handle.cancel()
    await cable.stop()
    await startTask.value

    #expect(ready == .ready)
    #expect(receivedPayload == .payload("9"))
  }

  @Test
  func subscriptionFailuresCarryTheirCause() async throws {
    let identifier = GraphQLChannel.identifier(channelId: "abc")
    let welcome = "{\"type\":\"welcome\"}"
    let confirmation = confirmationFrame(identifier: identifier)
    let ready = messageFrame(
      identifier: identifier,
      payload: #"{"requestId":"abc:1","subscriptionId":"s1","subscriptionReady":true,"more":true}"#
    )
    let graphqlError = try await firstFailure(
      frames: [
        welcome,
        confirmation,
        ready,
        messageFrame(identifier: identifier, payload: #"{"result":{"errors":[{"message":"boom"}]},"subscriptionId":"s1"}"#),
        rejectionFrame(identifier: identifier)
      ],
      scriptId: "network-transport-subscription-graphql"
    )
    let undecodable = try await firstFailure(
      frames: [
        welcome,
        confirmation,
        ready,
        messageFrame(identifier: identifier, payload: #"{"result":{"data":{"id":5}},"subscriptionId":"s1"}"#),
        rejectionFrame(identifier: identifier)
      ],
      scriptId: "network-transport-subscription-decode"
    )
    let rejected = try await firstFailure(
      frames: [welcome, rejectionFrame(identifier: identifier)],
      scriptId: "network-transport-subscription-rejected"
    )
    let completed = try await firstFailure(
      frames: [
        welcome,
        confirmation,
        ready,
        messageFrame(identifier: identifier, payload: #"{"result":{"data":null},"more":false,"subscriptionId":"s1"}"#)
      ],
      scriptId: "network-transport-subscription-completed"
    )

    #expect(graphqlError == SubscriptionFailure(kind: .graphql, message: "boom"))
    #expect(undecodable?.kind == .internalError)
    #expect(rejected?.kind == .graphql)
    #expect(completed?.kind == .transport)
  }

  private func firstFailure(frames: [String], scriptId: String) async throws -> SubscriptionFailure? {
    let cable = makeCableClient(socket: TestCableClientSocket(frames: frames))
    let (httpTransport, _) = try makeScriptedTransport(scriptId: scriptId)
    let transport: any GraphQLTransport = NetworkTransport(
      run: OperationRun(transport: httpTransport, hashes: PersistedQueryHashes()),
      cable: cable
    )
    let (callbacks, continuation) = AsyncStream<SubscriptionCallback>.makeStream()
    let handle = transport.subscribe(
      ProbeSubscription.self,
      variables: ProbeSubscription.Variables(),
      onReady: { continuation.yield(.ready) },
      onNext: { response in continuation.yield(.payload(response.data?.id ?? "")) },
      onError: { failure in continuation.yield(.error(failure)) }
    )
    let startTask = Task { await cable.start() }
    var iterator = callbacks.makeAsyncIterator()

    var callback = await iterator.next()
    if callback == .ready {
      callback = await iterator.next()
    }

    handle.cancel()
    await cable.stop()
    await startTask.value
    guard case let .error(failure) = callback else { return nil }
    return failure
  }
}

private func makeCableClient(
  socket: TestCableClientSocket = TestCableClientSocket(frames: [])
) -> CableClient {
  CableClient(
    url: URL(fileURLWithPath: "/test"),
    token: "token",
    makeSocket: { _, _ in socket },
    makeChannelId: { "abc" }
  )
}
