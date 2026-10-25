import DBlayerTesting
import Foundation
import Testing
@testable import DBlayer

private final class Collected: @unchecked Sendable {
  private let lock = NSLock()
  private var tasks: [PutTask] = []
  var all: [PutTask] { lock.withLock { tasks } }
  func add(_ new: [PutTask]) { lock.withLock { tasks += new } }
}

private func file(_ name: String = "part") -> PutFile { PutFile(path: "/tmp/\(name)", offset: 0, length: 5) }

private struct NoProgress: PutProgress {
  func sent(bytes: UInt64) {}
  func isCancelled() -> Bool { false }
}

struct BackgroundPutsTests {
  @Test
  func putStartsOneFileTaskWithTheTagAndAnswersWithTheEtag() async throws {
    let source = ScriptedUploadTasks()
    let puts = BackgroundPuts(source: source)
    let progress = NoProgress()
    async let reply = puts.put(url: "https://s3/p1", headers: ["x-a": "1"], file: file(), tag: "7.0.1", progress: progress)
    await eventually("the task starts") { !source.startedTasks.isEmpty }
    let started = try #require(source.startedTasks.first)
    #expect(started.tag == "7.0.1")
    #expect(started.file.path == "/tmp/part")
    #expect(started.request.httpMethod == "PUT")
    #expect(started.request.value(forHTTPHeaderField: "x-a") == "1")
    source.finish(tag: "7.0.1", etag: "etag-1")
    let answer = try await reply
    #expect(answer.status == 200 && answer.etag == "etag-1")
  }

  @Test
  func aLiveTaskAtOpenIsReportedToTheCoreAndAPutForItsTagStartsNothing() async throws {
    let source = ScriptedUploadTasks(live: ["7.0.2"])
    let puts = BackgroundPuts(source: source)
    let collected = Collected()
    puts.connect { collected.add($0) }
    async let reply = puts.put(url: "https://s3/p2", headers: [:], file: file(), tag: "7.0.2", progress: NoProgress())
    await eventually("the live task is reported") { collected.all.count == 1 }
    #expect(collected.all == [PutTask(tag: "7.0.2", state: .running)])
    #expect(source.startedTasks.isEmpty)
    source.finish(tag: "7.0.2", etag: "late")
    #expect(try await reply.etag == "late")
    #expect(source.startedTasks.isEmpty)
  }

  @Test
  func aTaskThatFinishedWhileNobodyWaitedGoesToTheCoreOnceItIsConnected() async throws {
    let source = ScriptedUploadTasks()
    let puts = BackgroundPuts(source: source)
    source.finish(tag: "7.0.3", etag: "dead-3")
    let collected = Collected()
    puts.connect { collected.add($0) }
    #expect(collected.all == [PutTask(tag: "7.0.3", state: .finished(status: 200, etag: "dead-3"))])
    source.fail(tag: "7.0.4", error: URLError(.networkConnectionLost))
    #expect(collected.all.count == 2)
  }

  @Test
  func aPutBeforeTheCoreIsConnectedTakesTheFinishedResultAndStartsNothing() async throws {
    let source = ScriptedUploadTasks()
    let puts = BackgroundPuts(source: source)
    source.finish(tag: "7.0.5", etag: "dead-5")
    let reply = try await puts.put(url: "https://s3/p5", headers: [:], file: file(), tag: "7.0.5", progress: NoProgress())
    #expect(reply.etag == "dead-5")
    #expect(source.startedTasks.isEmpty)
    let collected = Collected()
    puts.connect { collected.add($0) }
    #expect(collected.all.isEmpty)
  }

  @Test
  func aPutForATaskThatFailedWhileNobodyWaitedThrowsTheStoredNetworkFailure() async throws {
    let source = ScriptedUploadTasks()
    let puts = BackgroundPuts(source: source)
    source.fail(tag: "7.0.6", error: URLError(.networkConnectionLost))
    await #expect(throws: DbError.self) { try await puts.put(url: "https://s3/p6", headers: [:], file: file(), tag: "7.0.6", progress: NoProgress()) }
    #expect(source.startedTasks.isEmpty)
  }

  @Test
  func droppingTheCallerDoesNotCancelTheSystemTask() async throws {
    let source = ScriptedUploadTasks()
    let puts = BackgroundPuts(source: source)
    let caller = Task { try await puts.put(url: "https://s3/p5", headers: [:], file: file(), tag: "7.0.5", progress: NoProgress()) }
    await eventually("the task starts") { !source.startedTasks.isEmpty }
    caller.cancel()
    _ = await caller.result
    let again = Task { try await puts.put(url: "https://s3/p5", headers: [:], file: file(), tag: "7.0.5", progress: NoProgress()) }
    source.finish(tag: "7.0.5", etag: "kept")
    #expect(try await again.value.etag == "kept")
    #expect(source.startedTasks.count == 1)
    #expect(source.cancelledTags.isEmpty)
  }
}
