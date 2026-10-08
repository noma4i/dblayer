import Foundation
import Testing
@testable import DBlayer

@Suite(.serialized)
struct ServerClockTests {
  @Test
  func offsetFollowsServerDateHeader() async {
    let clock = ServerClock()
    let reference = Date(timeIntervalSince1970: 1_700_000_000)
    #expect(await clock.offset == 0)
    #expect(await clock.now(reference: reference) == reference)

    await clock.update(fromDateHeader: "Tue, 14 Nov 2023 22:14:20 GMT", receivedAt: reference)
    #expect(await clock.offset == 60)
    #expect(await clock.now(reference: reference) == Date(timeIntervalSince1970: 1_700_000_060))

    await clock.update(fromDateHeader: nil, receivedAt: reference)
    await clock.update(fromDateHeader: "not a date", receivedAt: reference)
    #expect(await clock.offset == 60)

    let behind = ServerClock()
    await behind.update(
      fromDateHeader: "Tue, 14 Nov 2023 22:13:20 GMT",
      receivedAt: Date(timeIntervalSince1970: 1_700_000_060)
    )
    #expect(await behind.offset == -60)
  }
}
