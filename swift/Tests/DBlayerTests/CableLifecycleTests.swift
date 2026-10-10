import Foundation
import Testing
@testable import DBlayer

@Suite(.serialized)
struct CableLifecycleTests {
  private enum CloseCause: CaseIterable {
    case serverDisconnectReconnectTrue
    case serverDisconnectReconnectFalse
    case requestedStop
  }

  @Test
  func silenceReopensAfterHalfSecond() async {
    let bench = CableBench()
    let connection = bench.makeConnection()
    await connection.open()
    await bench.clock.waitForSleepers(1)

    await bench.clock.advance(by: 6)
    await bench.clock.waitForSleepers(1, requestedSeconds: CableHeartbeat.reopenDelay)
    #expect(bench.sockets.count == 1)
    #expect(bench.sockets[0].isClosed)

    await bench.clock.advance(by: CableHeartbeat.reopenDelay)
    await eventually { bench.sockets.count == 2 }
    #expect(bench.sockets.count == 2)
    #expect(await connection.state == .open)
    await connection.close()
  }

  @Test
  func foregroundChecksStaleAfter200ms() async {
    let bench = CableBench()
    let connection = bench.makeConnection()
    await connection.open()
    bench.sockets[0].deliver("{\"type\":\"welcome\"}")
    await eventually { await connection.lastFrameAt != nil }
    #expect(await connection.lastFrameAt == bench.clock.now())

    let fresh = Task { await connection.foregroundCheck() }
    await bench.clock.waitForSleepers(1, requestedSeconds: CableHeartbeat.foregroundDelay)
    await bench.clock.advance(by: CableHeartbeat.foregroundDelay)
    await fresh.value
    await settle()
    #expect(bench.sockets.count == 1)

    await connection.closeSocket()
    let closed = Task { await connection.foregroundCheck() }
    await bench.clock.waitForSleepers(1, requestedSeconds: CableHeartbeat.foregroundDelay)
    #expect(bench.sockets.count == 1)
    await bench.clock.advance(by: CableHeartbeat.foregroundDelay)
    await closed.value
    await eventually { bench.sockets.count == 2 }

    #expect(bench.sockets.count == 2)
    await connection.close()
  }

  @Test
  func closeKeepsChannelsWhenWatchdogRuns() async throws {
    let bench = CableBench()
    let client = bench.makeClient()
    let stream = await client.subscribeGraphQL(document: "subscription X { x }", operationName: "X")
    let identifier = GraphQLChannel.identifier(channelId: "abc")
    let start = Task { await client.start() }

    await eventually { bench.sockets.count == 1 }
    bench.sockets[0].deliver("{\"type\":\"welcome\"}")
    await eventually { bench.sockets[0].sent.count == 1 }
    bench.sockets[0].deliver(confirmationFrame(identifier: identifier))
    await eventually { bench.sockets[0].sent.count == 2 }

    bench.sockets[0].fail()
    await eventually { bench.sockets[0].isClosed }
    await bench.clock.advance(by: 7)
    await bench.clock.waitForSleepers(1, requestedSeconds: CableHeartbeat.reopenDelay)
    await bench.clock.advance(by: CableHeartbeat.reopenDelay)
    await eventually { bench.sockets.count == 2 }
    bench.sockets[1].deliver("{\"type\":\"welcome\"}")
    await eventually { bench.sockets[1].sent.count == 1 }
    bench.sockets[1].deliver(confirmationFrame(identifier: identifier))
    await eventually { bench.sockets[1].sent.count == 2 }
    bench.sockets[1].deliver(
      messageFrame(identifier: identifier, payload: #"{"requestId":"abc:2","subscriptionId":"s2","subscriptionReady":true,"more":true}"#)
    )
    bench.sockets[1].deliver(
      messageFrame(identifier: identifier, payload: #"{"result":{"data":{"chatUpdated":{"id":"1"}}},"more":true,"subscriptionId":"s2"}"#)
    )

    #expect(bench.sockets[1].sent.first == CableCommand.subscribe(identifier: identifier).json)
    #expect(bench.sockets[1].sent.count == 2)
    var received = 0
    for try await event in stream {
      if case .payload = event {
        received += 1
        break
      }
    }
    #expect(received == 1)
    await client.stop()
    await start.value
  }

  @Test(arguments: CloseCause.allCases)
  private func closeCauseDecidesReopen(cause: CloseCause) async {
    let bench = CableBench()
    let client = bench.makeClient()
    let start = Task { await client.start() }
    await eventually { bench.sockets.count == 1 }

    switch cause {
    case .serverDisconnectReconnectTrue:
      bench.sockets[0].deliver(#"{"type":"disconnect","reason":"server_restart","reconnect":true}"#)
    case .serverDisconnectReconnectFalse:
      bench.sockets[0].deliver(#"{"type":"disconnect","reason":"unauthorized","reconnect":false}"#)
    case .requestedStop:
      await client.stop()
    }
    await eventually { bench.sockets[0].isClosed }
    await bench.clock.advance(by: 7)
    if cause == .serverDisconnectReconnectTrue {
      await bench.clock.waitForSleepers(1, requestedSeconds: CableHeartbeat.reopenDelay)
      await bench.clock.advance(by: CableHeartbeat.reopenDelay)
    }
    await settle()

    #expect(bench.sockets.count == (cause == .serverDisconnectReconnectTrue ? 2 : 1))
    await client.stop()
    await start.value
  }

  @Test
  func tokenChangeRecreatesConnectionWithNewHeader() async {
    let bench = CableBench(channelIds: ["old", "old-op", "new", "new-op"])
    let client = bench.makeClient()
    _ = await client.subscribeGraphQL(document: "subscription X { x }", operationName: "X")
    let start = Task { await client.start() }
    await eventually { bench.sockets.count == 1 }
    bench.sockets[0].deliver("{\"type\":\"welcome\"}")
    await eventually { bench.sockets[0].sent.count == 1 }
    bench.failOpens = 1

    let replace = Task { await client.replaceToken("fresh") }
    await bench.clock.waitForSleepers(1, requestedSeconds: CableReconnect.mergeDelay)
    await bench.clock.advance(by: CableReconnect.mergeDelay)
    await bench.clock.waitForSleepers(1, requestedSeconds: 1)
    await bench.clock.advance(by: 1)
    await replace.value
    await eventually { bench.sockets.count == 3 }
    bench.sockets[2].deliver("{\"type\":\"welcome\"}")
    await eventually { bench.sockets[2].sent.count == 1 }

    #expect(bench.log == ["open old-token", "close 0", "open fresh", "open fresh"])
    #expect(bench.sockets[2].sent == [CableCommand.subscribe(identifier: GraphQLChannel.identifier(channelId: "new")).json])
    await client.stop()
    await start.value
  }
}
