import Foundation
import Testing
@testable import DBlayer

private final class Reports: @unchecked Sendable {
  private let lock = NSLock()
  private var values: [Bool] = []
  var all: [Bool] { lock.withLock { values } }
  func add(_ value: Bool) { lock.withLock { values.append(value) } }
}

private final class Fact: @unchecked Sendable {
  private let lock = NSLock()
  private var value = false
  var current: Bool {
    get { lock.withLock { value } }
    set { lock.withLock { value = newValue } }
  }
}

struct PowerSavingTests {
  @Test
  func theOsValueReachesTheCoreAtStartAndOnEveryChangeUntilStopped() {
    let name = Notification.Name("DBlayerTestPowerState")
    let fact = Fact()
    let reports = Reports()
    fact.current = true
    let stop = PowerSaving.observe(name: name, read: { fact.current }) { reports.add($0) }
    fact.current = false
    NotificationCenter.default.post(name: name, object: nil)
    stop()
    NotificationCenter.default.post(name: name, object: nil)
    #expect(reports.all == [true, false])
  }
}
