import DBlayerTesting
import Foundation
import Testing
@testable import DBlayer

private final class Heard: ValueSink, @unchecked Sendable {
  private let lock = NSLock()
  private var heard: UInt64 = 0
  var count: UInt64 { lock.withLock { heard } }
  func available() { lock.withLock { heard += 1 } }
}

private final class CallBox: @unchecked Sendable {
  private let lock = NSLock()
  private var value: CallResult?
  var done: CallResult? { lock.withLock { value } }
  func finish(_ result: CallResult) { lock.withLock { value = result } }
}

/// What a finished call hands back to the runner: what to observe and what to remember under a name.
private struct CallResult: Sendable {
  var observed: [String] = []
  var row: (String, String)?
  var receipt: (String, WireOperationReceipt)?
}

private func fail(_ error: DbError) -> CallResult { CallResult(observed: failure(kind(of: error))) }

private func failure(_ kind: String) -> [String] { [observation("error", ["kind": kind])] }

private func kind(of error: DbError) -> String {
  switch error {
  case .NoSession: return "no_session"
  case .Security: return "security"
  case .Config: return "config"
  case .Invalid: return "invalid"
  case .Database: return "database"
  case .Sync: return "sync"
  case .Network: return "network"
  case .Interrupted: return "interrupted"
  case .Http: return "http"
  case .RequestTooLarge: return "request_too_large"
  case .Graphql: return "graphql"
  case .Session: return "session"
  }
}

private func done(_ kind: String, _ value: Any? = nil) -> CallResult { CallResult(observed: [observation(kind, value)]) }

private func failed(_ error: Error) -> CallResult { (error as? DbError).map(fail) ?? CallResult(observed: failure("invalid")) }

private func phaseName(_ phase: LivePhase) -> String {
  switch phase {
  case .notLoaded, .closed: return "idle"
  case .loading: return "loading"
  case .loaded, .empty: return "loaded"
  case .failed: return "failed"
  }
}

private func causeName(_ end: StreamEnd) -> String {
  switch end {
  case .sessionEnded: return "session_ended"
  case .closed: return "closed"
  case .invalid: return "invalid"
  case .outOfSync: return "out_of_sync"
  }
}

private func fetchOf(_ name: String) -> EntityFetch? {
  switch name {
  case "never": return EntityFetch.none
  case "stale": return .stale
  case "force": return .force
  case "if_missing": return .ifMissing
  default: return nil
  }
}

private func outcomeName(_ outcome: WireOperationOutcome) -> String {
  switch outcome {
  case .confirmed: return "confirmed"
  case .rejected: return "rejected"
  case .deliveryUnknown: return "delivery_unknown"
  case .interrupted: return "interrupted"
  }
}

private func moreName(_ result: LoadMoreResult) -> String {
  switch result {
  case .loaded(let hasMore): return hasMore ? "loaded_more" : "loaded_last"
  case .noMore: return "no_more"
  case .alreadyLoading: return "already_loading"
  }
}

private func textOr(_ args: [String: Any], _ key: String, _ fallback: String) -> String { args[key] as? String ?? fallback }

private func modelsText(_ file: String) throws -> String { try String(contentsOf: corpusDirectory().appendingPathComponent(file), encoding: .utf8) }

private func corpusDirectory() -> URL {
  URL(fileURLWithPath: #filePath).deletingLastPathComponent().deletingLastPathComponent().deletingLastPathComponent().deletingLastPathComponent().appendingPathComponent("core/tests/corpus")
}

/// Runs the inputs of `specs/machines/corpus.md` through the public API of `Database`.
private final class DatabaseRunner {
  var database: Database?
  let mock = CorpusMock()
  let clock = TestClock(startMillis: 1_800_000_000_000)
  let heard = Heard()
  var rows: [String: String] = [:]
  var receipts: [String: WireOperationReceipt] = [:]
  var loads: [String: LoadRequest] = [:]
  var streams: [String: UInt64] = [:]
  var calls: [CallBox] = []

  func idOf(_ name: String) -> String { rows[name] ?? name }

  func nameOf(_ id: String) -> String { rows.first { $0.value == id }?.key ?? id }

  func names(_ list: [WireRow]) -> [String] { list.map { nameOf($0.id) } }

  func stateFields(_ stream: String, _ state: StreamState) -> [String: Any] {
    var out: [String: Any] = ["stream": stream, "phase": phaseName(state.phase)]
    if let kind = state.errorKind { out["error"] = kind }
    if let at = state.loadedAt { out["loaded"] = at }
    return out
  }

  func value(_ stream: String, _ seq: UInt64, _ value: StreamValue) -> String {
    switch value {
    case .snapshot(let revision, let list, let state):
      var out = stateFields(stream, state); out["revision"] = revision; out["rows"] = names(list)
      return observation("snapshot", out)
    case .delta(let base, let revision, _, let list, _, let removed, _, _, _, let state):
      var out = stateFields(stream, state); out["base"] = base; out["revision"] = revision; out["rows"] = names(list); out["removed"] = removed.map(nameOf)
      return observation("delta", out)
    case .count(let revision, let count, let state):
      var out = stateFields(stream, state); out["revision"] = revision; out["count"] = count
      return observation("count", out)
    case .state(let state):
      return observation("state", stateFields(stream, state))
    case .event(let event):
      return observation("event", ["stream": stream, "position": seq, "area": event.area, "kind": event.kind, "row": nameOf(event.id)])
    case .lost(let first, let last):
      return observation("lost", ["stream": stream, "first": first, "last": last])
    case .status:
      return observation("unsupported", "status")
    case .ended(let reason):
      return observation("ended", ["stream": stream, "cause": causeName(reason)])
    }
  }

  func spawn(_ work: @escaping @Sendable () async -> CallResult) {
    let box = CallBox()
    calls.append(box)
    Task { box.finish(await work()) }
  }

  func settle() async -> [String] {
    var quiet = 0
    var last = (-1, -1, UInt64.max)
    for _ in 0..<600 {
      try? await Task.sleep(for: .milliseconds(10))
      let now = (mock.count, calls.filter { $0.done != nil }.count, heard.count)
      quiet = now == last ? quiet + 1 : 0
      last = now
      if quiet >= 25 { break }
    }
    var observed: [String] = []
    var waiting: [CallBox] = []
    for call in calls {
      guard let result = call.done else { waiting.append(call); continue }
      observed += result.observed
      if let (name, id) = result.row { rows[name] = id }
      if let (name, receipt) = result.receipt { receipts[name] = receipt }
    }
    calls = waiting
    return observed + mock.announce()
  }

  func open(_ models: String) async -> [String] {
    do {
      let opened = try await OpenDatabase().openWithClock(config: Config(path: nil, dataVersion: nil, cacheRows: 32), modelsJson: try modelsText(models == "events" ? "models_events.json" : "models.json"), transport: mock, uploads: nil, clock: clock, preload: false, listening: true, encrypted: nil, credentials: nil, http: nil, retries: nil)
      let operations = try String(contentsOf: corpusDirectory().appendingPathComponent("operations.json"), encoding: .utf8)
      try opened.registerOperations(defsJson: operations)
      database = opened
      return [observation("opened")]
    } catch { return failure("open") }
  }

  static func receiptDone(_ name: String, _ row: String, _ op: String, _ result: () async throws -> WireOperationReceipt) async -> CallResult {
    do {
      let receipt = try await result()
      return CallResult(observed: [observation("receipt", ["row": row, "op": op])], receipt: (name, receipt))
    } catch { return failed(error) }
  }

  func start(_ database: Database, _ name: String, _ args: [String: Any]) -> [String]? {
    let epoch = database.sessionEpoch()
    let text = { (key: String) in args[key] as? String }
    switch name {
    case "create":
      guard let row = text("as"), let body = text("text") else { return failure("arguments") }
      spawn {
        do {
          let receipt = try await database.send(request: CommandRequest(epoch: epoch, model: "Moment", valuesJson: canonical(["text": body]), argsJson: "{}", upload: nil))
          var result = CallResult(observed: [observation("receipt", ["row": row, "op": "create"])], receipt: (row, receipt))
          if let created = receipt.optimisticValue { result.row = (row, created.id) }
          return result
        } catch { return failed(error) }
      }
    case "update":
      guard let row = text("row"), let body = text("text") else { return failure("arguments") }
      let id = idOf(row), label = textOr(args, "as", "\(row):update")
      spawn { await Self.receiptDone(label, row, "update") { try await database.update(epoch: epoch, model: "Moment", rowId: id, patchJson: canonical(["text": body]), argsJson: "{}") } }
    case "act":
      guard let row = text("row"), let action = text("action") else { return failure("arguments") }
      let id = idOf(row), label = textOr(args, "as", "\(row):\(action)")
      spawn { await Self.receiptDone(label, row, "act") { try await database.act(epoch: epoch, model: "Moment", rowId: id, action: action, patchJson: "{}", argsJson: "{}") } }
    case "destroy":
      guard let row = text("row") else { return failure("arguments") }
      let id = idOf(row), label = textOr(args, "as", "\(row):destroy")
      spawn { await Self.receiptDone(label, row, "destroy") { try await database.destroy(epoch: epoch, model: "Moment", rowId: id) } }
    case "retry":
      guard let row = text("row") else { return failure("arguments") }
      let id = idOf(row), action = text("action"), label = textOr(args, "as", "\(row):retry")
      spawn { await Self.receiptDone(label, row, "retry") { try await database.retry(epoch: epoch, model: "Moment", rowId: id, action: action, source: nil) } }
    case "discard":
      guard let row = text("row") else { return failure("arguments") }
      let id = idOf(row), action = text("action")
      spawn {
        do { try await database.discard(epoch: epoch, model: "Moment", rowId: id, action: action); return done("discarded", ["row": row]) } catch { return failed(error) }
      }
    case "upload":
      guard let row = text("row"), let action = text("action") else { return failure("arguments") }
      let id = idOf(row), label = textOr(args, "as", "\(row):\(action)")
      let path = FileManager.default.temporaryDirectory.appendingPathComponent("corpus_\(ProcessInfo.processInfo.processIdentifier).bin")
      guard (try? Data("corpus".utf8).write(to: path)) != nil else { return failure("file") }
      let request = WireUploadRequest(path: path.path, name: "clip.bin", mime: "application/octet-stream", variablesJson: "{}")
      spawn { await Self.receiptDone(label, row, "upload") { try await database.upload(epoch: epoch, model: "Moment", rowId: id, action: action, request: request) } }
    case "cancel_upload":
      guard let row = text("row"), let action = text("action") else { return failure("arguments") }
      let id = idOf(row)
      spawn {
        do { return done("cancelled", ["row": row, "value": try await database.cancelUpload(epoch: epoch, model: "Moment", rowId: id, action: action)]) } catch { return failed(error) }
      }
    case "outcome":
      guard let label = text("receipt") else { return failure("arguments") }
      guard let receipt = receipts[label] else { return failure("unknown_receipt") }
      spawn {
        do { return done("outcome", ["receipt": label, "result": outcomeName(try await database.outcome(receipt: receipt))]) } catch { return failed(error) }
      }
    case "begin_load":
      guard let label = text("as"), let relation = text("relation") else { return failure("arguments") }
      do {
        let load = try database.beginLoad(epoch: epoch, model: "Moment", relation: relation, paramsJson: "{}")
        loads[label] = load
        spawn {
          do { try await load.result(); return done("loaded", ["load": label]) } catch { return failed(error) }
        }
      } catch { return failure(kind(of: error as? DbError ?? .Invalid(detail: ""))) }
    case "refresh":
      guard let relation = text("relation") else { return failure("arguments") }
      spawn {
        do { try await database.refresh(epoch: epoch, model: "Moment", relation: relation, paramsJson: "{}"); return done("refreshed", ["relation": relation]) } catch { return failed(error) }
      }
    case "load_more":
      guard let relation = text("relation") else { return failure("arguments") }
      spawn {
        do { return done("more", ["relation": relation, "result": moreName(try await database.loadMore(epoch: epoch, model: "Moment", relation: relation, paramsJson: "{}"))]) } catch { return failed(error) }
      }
    case "node":
      guard let relation = text("relation"), let row = text("row") else { return failure("arguments") }
      let id = idOf(row)
      spawn {
        do { return done("node", ["row": row, "present": try await database.node(epoch: epoch, model: "Moment", relation: relation, rowId: id, force: false) != nil]) } catch { return failed(error) }
      }
    case "session_call":
      guard let operation = text("operation"), let body = args["body"] else { return failure("arguments") }
      let json = canonical(body)
      spawn {
        do {
          let answer = try await database.sessionCall(operation: operation, bodyJson: json)
          let parsed = (try? JSONSerialization.jsonObject(with: Data(answer.utf8), options: [.fragmentsAllowed])) ?? NSNull()
          return done("session", ["operation": operation, "answer": parsed])
        } catch { return failed(error) }
      }
    case "session_reset":
      spawn {
        do { try await database.sessionReset(); return done("reset") } catch { return failed(error) }
      }
    case "close":
      mock.releaseAll()
      spawn {
        do { try await database.closeDatabase(); return done("closed") } catch { return failed(error) }
      }
    case "has":
      guard let row = text("row") else { return failure("arguments") }
      let id = idOf(row)
      spawn {
        do { let found = try await database.readQuery(epoch: epoch, queryJson: #"{"model":"Moment"}"#); return done("row", ["row": row, "present": found.contains { $0.id == id }]) } catch { return failed(error) }
      }
    default:
      return nil
    }
    return []
  }

  func openStream(_ database: Database, _ name: String, _ args: [String: Any]) -> [String] {
    guard let label = args["as"] as? String else { return failure("arguments") }
    func query(_ relation: String?) -> String {
      if let relation { return canonical(["model": "Moment", "relation": relation, "params": [String: Any]()] as [String: Any]) }
      return canonical(["model": "Moment"])
    }
    let request: StreamRequest
    switch name {
    case "open_list": request = .query(queryJson: query(args["relation"] as? String))
    case "open_count": request = .count(queryJson: query(args["relation"] as? String))
    case "open_entity":
      guard let row = args["row"] as? String, let fetch = (args["fetch"] as? String).flatMap(fetchOf) else { return failure("arguments") }
      request = .entity(model: "Moment", id: idOf(row), fetch: fetch)
    default:
      guard let model = args["model"] as? String else { return failure("arguments") }
      request = .events(model: model)
    }
    streams[label] = database.subscribe(epoch: database.sessionEpoch(), request: request, sink: heard)
    return []
  }

  func apply(_ input: [String: Any]) async -> [String] {
    let name = input["in"] as? String ?? ""
    let args = input["args"] as? [String: Any] ?? [:]
    switch name {
    case "open": return await open(args["models"] as? String ?? "")
    case "tick":
      clock.advance(millis: (args["ms"] as? NSNumber)?.uint64Value ?? 0)
      return await settle()
    case "answer", "error", "drop", "ready", "frame":
      guard let id = args["id"] as? String else { return failure("arguments") }
      var reached = false
      switch name {
      case "answer":
        guard let body = args["json"] else { return failure("arguments") }
        reached = mock.answer(id, canonical(body))
      case "error":
        guard let kind = args["kind"] as? String else { return failure("arguments") }
        reached = mock.fail(id, kind)
      case "drop": reached = mock.dropConnection(id)
      case "ready": reached = mock.ready(id)
      default:
        guard let event = args["event"] else { return failure("arguments") }
        do { reached = try mock.frame(id, canonical(event)) } catch { return failure("frame") }
      }
      if !reached { return failure("no_request") }
      return await settle()
    case "lost": return [observation("unsupported", name)]
    default: break
    }
    guard let database else { return failure("not_open") }
    var observed: [String]
    switch name {
    case "online":
      if let sink = try? database.onlineSink() { sink.setOnline(online: args["online"] as? Bool ?? false); observed = [] } else { observed = failure("online") }
    case "set_online":
      let online = args["online"] as? Bool ?? false
      do { try database.setOnline(online: online); observed = [observation("online", online)] } catch { observed = failure("online") }
    case "published":
      observed = [observation("published", ["epoch": database.sessionEpoch()])]
    case "open_list", "open_count", "open_entity", "open_events":
      observed = openStream(database, name, args)
    case "pull":
      guard let label = args["stream"] as? String else { return failure("arguments") }
      guard let id = streams[label] else { return failure("unknown_stream") }
      if let delivery = database.readStream(stream: id) { observed = [value(label, delivery.seq, delivery.value)] } else { observed = [observation("empty", ["stream": label])] }
    case "cancel":
      guard let target = args["target"] as? String else { return failure("arguments") }
      if let load = loads[target] { load.cancel() } else if let id = streams[target] { database.cancelStream(stream: id) } else { return failure("unknown_target") }
      observed = []
    default:
      guard let immediate = start(database, name, args) else { return [observation("unsupported", name)] }
      observed = immediate
    }
    observed += await settle()
    if name == "close" && calls.isEmpty { self.database = nil }
    return observed
  }
}

private func corpusFiles() throws -> [URL] {
  try FileManager.default.contentsOfDirectory(at: corpusDirectory(), includingPropertiesForKeys: nil).filter { $0.pathExtension == "jsonl" }.sorted { $0.lastPathComponent < $1.lastPathComponent }
}

@Suite(.serialized)
struct CorpusRunnerTests {
  @Test(.timeLimit(.minutes(5)))
  func theSharedCorpusIsGreenThroughTheSwiftWrapper() async throws {
    let files = try corpusFiles()
    #expect(!files.isEmpty)
    for file in files {
      let lines = try String(contentsOf: file, encoding: .utf8).split(separator: "\n").filter { !$0.allSatisfy(\.isWhitespace) }
      let runner = DatabaseRunner()
      for (index, line) in lines.dropFirst().enumerated() {
        let step = try #require(try JSONSerialization.jsonObject(with: Data(line.utf8)) as? [String: Any])
        let got = "[" + (await runner.apply(step)).joined(separator: ",") + "]"
        let expected = canonical(step["out"] ?? [])
        #expect(got == expected, "\(file.lastPathComponent) step \(index + 1) input \(canonical(step["in"] ?? "")) expected \(expected) got \(got)")
      }
      runner.mock.releaseAll()
      if let database = runner.database { try? await database.closeDatabase() }
    }
  }
}
