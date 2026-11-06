import DBlayerTesting
import Foundation
import Testing
@testable import DBlayer

private struct PhoneBody: Encodable, Sendable { let phone: String }

extension BridgeProbeTests {
  @MainActor
  @Test(.timeLimit(.minutes(1)))
  func readme_examples_run_on_both_platforms() async throws {
    await DB.closeForCleanup()
    let transport = MockTransport()
    let created = #"{"data":{"createMoment":{"__typename":"Moment","id":"m1","uuid":"u","userId":"u1","text":"hello","likedByMe":false,"media":{},"createdAt":"2026-10-08T00:00:00.000Z"}}}"#
    transport.enqueue("CreateMoment", reply: created)
    try await reopen(config: Config(path: nil, dataVersion: nil, cacheRows: 32), modelsJson: modelsJSON, transport: transport, operations: sendMoment)
    let scope = try DB.account()

    let moments: Projected<Moment> = Projected(try scope.stream(Moment.where(Moment.userId == "u1").order(Moment.createdAt.desc).limit(20)))
    let moment = try await Moment.send(in: scope, text: "hello").optimisticValue
    #expect(moment?.text == "hello")
    await eventually("the row of the sent moment reaches the feed") { moments.rows.map(\.text) == ["hello"] }

    let user = try #require(try await User.create(name: "ann", in: scope).optimisticValue)
    let bytes = Data([1, 2, 3])
    let info = try await user.attach(.avatar, data: bytes, name: "a.png", mime: "image/png")
    #expect((info.name, info.mime, info.size) == ("a.png", "image/png", 3))

    try await DB.sessionCall("login_link", body: PhoneBody(phone: "+61400000000"))
  }
}
