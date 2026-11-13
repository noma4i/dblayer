import DBlayerTesting
import Testing
@testable import DBlayer

extension BridgeProbeTests {
  @Test func theQueryBuilderFillsTheRecordTheCoreCompiles() throws {
    let number = Column<Moment, Int>("n")
    let conditions: [(Condition, QueryCondition)] = [
      (try number == 3, .equal(field: "n", value: .int(value: 3))),
      (try number != 3, .notEqual(field: "n", value: .int(value: 3))),
      (try number < 3, .compare(field: "n", op: .less, value: .int(value: 3))),
      (try number <= 3, .compare(field: "n", op: .lessOrEqual, value: .int(value: 3))),
      (try number > 3, .compare(field: "n", op: .greater, value: .int(value: 3))),
      (try number >= 3, .compare(field: "n", op: .greaterOrEqual, value: .int(value: 3))),
      (try number.in([1, 2]), .inList(field: "n", values: [.int(value: 1), .int(value: 2)])),
      (number.isNull, .isNull(field: "n")),
      (.search("hi", wholeWords: true), .search(text: "hi", wholeWords: true)),
      (.equal("t", .null) || .equal("t", .string("x")), .anyOf(clauses: [.equal(field: "t", value: .null), .equal(field: "t", value: .text(value: "x"))])),
      (!.isNull("t"), .not(clause: .isNull(field: "t"))),
    ]
    for (condition, expected) in conditions { #expect(condition.tree == expected) }

    let orders: [(Order, SortDirection)] = [(number.asc, .asc), (number.desc, .desc), (number.ascNullsLast, .ascNullsLast), (number.descNullsFirst, .descNullsFirst)]
    for (order, direction) in orders { #expect(order == QueryOrder(field: "n", direction: direction)) }

    let spec = try Query<Moment>(relation: "feed", params: ["k": .string("v")]).where(try number > 1).where(.isNull("t")).order(number.asc).limit(5).offset(2).spec(refreshOnConnect: true)
    #expect(spec == QuerySpec(model: "Moment", relation: "feed", paramsJson: #"{"k":"v"}"#, conditions: [.compare(field: "n", op: .greater, value: .int(value: 1)), .isNull(field: "t")], order: [number.asc], limit: 5, offset: 2, refreshOnConnect: true))
  }

  @Test @MainActor func aStreamWithoutARelationRefusesToLoad() async throws {
    try await openSignedOffline(config: Config(path: nil), modelsJson: modelsJSON)
    let stream = try DB.account().stream(Query<Moment>())
    await expectDbError("Invalid") { try await stream.refresh() }
    await expectDbError("Invalid") { try await stream.loadMore() }
    await stream.cancel()
  }
}
