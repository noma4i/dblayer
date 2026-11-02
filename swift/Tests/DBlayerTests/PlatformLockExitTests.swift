import Foundation
import Testing
@testable import DBlayer

private func finishesElsewhere(_ probe: @escaping @Sendable () -> Void) -> Bool {
  let done = DispatchSemaphore(value: 0)
  Thread.detachNewThread { probe(); done.signal() }
  return done.wait(timeout: .now() + .seconds(10)) == .success
}

private final class Journal: @unchecked Sendable {
  private let lock = NSLock()
  private var lines: [String] = []
  private var checks: [Bool] = []
  var recorded: [String] { lock.withLock { lines } }
  var probes: [Bool] { lock.withLock { checks } }
  func add(_ line: String) { lock.withLock { lines.append(line) } }
  func probed(_ finished: Bool) { lock.withLock { checks.append(finished) } }
}

private final class Holder<Value: AnyObject>: @unchecked Sendable {
  private let lock = NSLock()
  private var stored: Value?
  var value: Value? { get { lock.withLock { stored } } set { lock.withLock { stored = newValue } } }
}

private final class Hooks: @unchecked Sendable {
  private let lock = NSLock()
  private var decoding: (@Sendable () -> Void)?
  private var released: (@Sendable () -> Void)?
  func onDecode(_ body: (@Sendable () -> Void)?) { lock.withLock { decoding = body } }
  func onRelease(_ body: (@Sendable () -> Void)?) { lock.withLock { released = body } }
  func decode() { lock.withLock { decoding }?() }
  func release() { lock.withLock { released }?() }
}

private final class Sentinel: @unchecked Sendable {
  deinit { ProbedMoment.hooks.release() }
}

private struct ProbedMoment: DBModel {
  static let hooks = Hooks()
  static let modelName = Moment.modelName
  static let wireLayout = Moment.wireLayout
  let value: Moment
  let sentinel = Sentinel()
  var id: String { value.id }
  var sync: SyncInfo { value.sync }
  var uploads: [String: UploadInfo] { value.uploads }
  init(wire: WireRow) throws { Self.hooks.decode(); value = try Moment(wire: wire) }
  static func == (left: ProbedMoment, right: ProbedMoment) -> Bool { left.id == right.id }
}

private func wire(_ id: String) -> WireRow {
  WireRow(id: id, revision: 1, estimatedBytes: 1, layout: Moment.wireLayout,
    cells: [.text(value: id), .null, .text(value: id), .bool(value: false), .keyed(entries: []), .null, .text(value: "x")],
    sync: WireSync(state: .synced, actions: [], dirtyFields: [], error: nil, attempts: 0, since: 0), uploads: [])
}

private func full(_ ids: [String]) -> LiveChange {
  LiveChange(revision: 1, full: true, size: UInt32(ids.count), rows: ids.map(wire), rowIndexes: [], removed: [], placed: [], replaced: [], renamed: [], count: nil, error: nil)
}

@Suite(.serialized)
struct PlatformLockExitTests {
  @Test
  func theOnlineRelayDeliversToItsReceiversWithNoRelayLockHeld() {
    let journal = Journal()
    let holder = Holder<OnlineRelay>()
    let relay = OnlineRelay { online in
      journal.add("connectivity:\(online)")
      if online { journal.probed(finishesElsewhere { _ = holder.value?.set(false) }) }
    }
    holder.value = relay
    relay.attach { journal.add("sink:\($0)") }
    relay.set(true)

    #expect(journal.probes == [true])
    #expect(journal.recorded.last == "sink:false")
    #expect(journal.recorded.filter { $0.hasPrefix("connectivity") }.last == "connectivity:false")
    holder.value = nil
  }

  @Test
  func aMergeRunsWithNoLatestLockHeldAndALateWriterIsNotLost() async {
    let journal = Journal()
    let holder = Holder<Latest<Int>>()
    let latest = Latest<Int>(merge: { older, newer in
      guard journal.probes.isEmpty else { return older + newer }
      journal.probed(finishesElsewhere { _ = holder.value?.hasPending })
      holder.value?.put(100)
      return older + newer
    })
    holder.value = latest
    latest.put(1)
    latest.put(2)

    #expect(journal.probes == [true])
    #expect(await latest.next() == 103)
    holder.value = nil
  }

  @Test
  func aRowDecoderRunsWithNoFeedLockHeld() {
    let journal = Journal()
    let holder = Holder<ListFeed<ProbedMoment>>()
    let feed = ListFeed<ProbedMoment>()
    holder.value = feed
    ProbedMoment.hooks.onDecode { journal.probed(finishesElsewhere { _ = holder.value?.rows }) }
    let applied = feed.apply(full(["a", "b"]))
    ProbedMoment.hooks.onDecode(nil)

    #expect(journal.probes == [true, true])
    #expect(applied.rows.map(\.id) == ["a", "b"])
    holder.value = nil
  }

  @Test
  func aReplacedRowIsReleasedWithNoFeedLockHeld() {
    let journal = Journal()
    let holder = Holder<ListFeed<ProbedMoment>>()
    let feed = ListFeed<ProbedMoment>()
    holder.value = feed
    var first: ListApplied<ProbedMoment>? = feed.apply(full(["a"]))
    #expect(first?.rows.map(\.id) == ["a"])
    first = nil
    ProbedMoment.hooks.onRelease { journal.probed(finishesElsewhere { _ = holder.value?.rows }) }
    let second = feed.apply(full(["b"]))
    ProbedMoment.hooks.onRelease(nil)

    #expect(journal.probes == [true])
    #expect(second.rows.map(\.id) == ["b"])
    holder.value = nil
  }

  @Test
  func theCancelOfAnInnerStreamRunsWithNoReaderLockHeld() {
    let journal = Journal()
    let holder = Holder<StreamBox>()
    let inner = LiveStream<Int>(receive: { nil }, cancel: { journal.probed(finishesElsewhere { holder.value?.stream.cancel() }) })
    let outer = LiveStream<Int>(first: inner, reopen: { .ended })
    holder.value = StreamBox(outer)
    outer.cancel()

    #expect(journal.probes == [true])
    holder.value = nil
  }
}

private final class StreamBox: @unchecked Sendable {
  let stream: LiveStream<Int>
  init(_ stream: LiveStream<Int>) { self.stream = stream }
}
