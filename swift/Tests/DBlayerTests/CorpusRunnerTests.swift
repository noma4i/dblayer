import DBlayerTesting
import Foundation
import Testing
@testable import DBlayer

private final class Heard: @unchecked Sendable {
  private let lock = NSLock()
  private var heard: UInt64 = 0
  var count: UInt64 { lock.withLock { heard } }
  func add() { lock.withLock { heard += 1 }; TestPulse.shared.pulse() }
}

/// A stream the runner reads: a task pulls it into a queue, `pull` of the corpus folds the unread values.
private final class Pulled: @unchecked Sendable {
  let stream: ValueStream
  private let lock = NSLock()
  private var queue: [ReadValue] = []
  private var task: Task<Void, Never>?

  init(_ stream: ValueStream, heard: Heard) {
    self.stream = stream
    task = Task { [self] in
      do {
        for try await value in stream {
          lock.withLock { queue.append(value) }
          heard.add()
        }
      } catch {}
    }
  }

  deinit { task?.cancel() }

  func peek() -> [ReadValue] { lock.withLock { queue } }

  func drain() -> [ReadValue] {
    lock.withLock {
      defer { queue.removeAll() }
      return queue
    }
  }
}

private enum Folded {
  case delta(Int64, Int64, [String], [String], ReadState)
  case value(ReadValue)

  var isState: Bool {
    if case .value(.state) = self { true } else { false }
  }

  var survivesSnapshot: Bool {
    switch self {
    case .delta: false
    case .value(.state), .value(.snapshot), .value(.count): false
    case .value: true
    }
  }
}

private final class Credentials: CredentialStore, @unchecked Sendable {
  private let lock = NSLock()
  private var value: String?
  func get() throws -> String? { lock.withLock { value } }
  func set(credential: String) throws { lock.withLock { value = credential } }
  func clear() throws { lock.withLock { value = nil } }
}

private final class CallBox: @unchecked Sendable {
  private let lock = NSLock()
  private var value: CallResult?
  var done: CallResult? { lock.withLock { value } }
  func finish(_ result: CallResult) { lock.withLock { value = result }; TestPulse.shared.pulse() }
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
  case .NotFound: return "not_found"
  case .Storage: return "storage"
  case .Offline: return "offline"
  case .Dropped: return "dropped"
  case .Unauthorized: return "unauthorized"
  case .InFlight: return "in_flight"
  case .Runtime: return "runtime"
  case .Internal: return "internal"
  case .Copy: return "copy"
  case .Network: return "network"
  case .Interrupted: return "interrupted"
  case .Http: return "http"
  case .RequestTooLarge: return "request_too_large"
  case .ResponseTooLarge: return "response_too_large"
  case .Graphql: return "graphql"
  case .Session: return "session"
  case .Closed: return "closed"
  case .CorruptManifest: return "corrupt_manifest"
  case .NotOpen: return "not_open"
  case .AlreadyExists: return "already_exists"
  case .Rejected: return "rejected"
  }
}

private func done(_ kind: String, _ value: Any? = nil) -> CallResult { CallResult(observed: [observation(kind, value)]) }

private func failed(_ error: Error) -> CallResult { (error as? DbError).map(fail) ?? CallResult(observed: failure("invalid")) }

private func phaseName(_ phase: ReadPhase) -> String {
  switch phase {
  case .idle: return "idle"
  case .loading: return "loading"
  case .loaded: return "loaded"
  case .failed: return "failed"
  }
}

private func causeName(_ end: EndCause) -> String {
  switch end {
  case .sessionEnded: return "session_ended"
  case .closed: return "closed"
  case .cancelled: return "cancelled"
  case .discarded: return "discarded"
  case .invalid: return "invalid"
  case .storage: return "storage"
  case .internal: return "internal"
  }
}

private func fetchOf(_ name: String) -> ReadFetch? {
  switch name {
  case "never": return ReadFetch.never
  case "stale": return .stale
  case "force": return .force
  case "if_missing": return .ifMissing
  default: return nil
  }
}

private func outcomeName(_ database: Database, _ receipt: WireOperationReceipt) async -> String {
  do {
    _ = try await database.confirmed(receipt: receipt, discardOnFailure: false)
    return "confirmed"
  } catch DbError.Rejected { return "rejected"
  } catch DbError.NoSession { return "ended session_ended"
  } catch DbError.Closed { return "ended closed"
  } catch DbError.Invalid { return "ended invalid"
  } catch DbError.Storage { return "ended storage"
  } catch DbError.Internal { return "ended internal"
  } catch let DbError.Interrupted(detail) { return "ended \(detail.lowercased())"
  } catch { return "failed \(error)" }
}

private func moreName(_ result: LoadMoreResult) -> String {
  switch result {
  case .loaded(let hasMore): return hasMore ? "loaded_more" : "loaded_last"
  case .noMore: return "no_more"
  }
}

private func textOr(_ args: [String: Any], _ key: String, _ fallback: String) -> String { args[key] as? String ?? fallback }

private func modelsText(_ file: String) throws -> String { try String(contentsOf: corpusDirectory().appendingPathComponent(file), encoding: .utf8) }

private func corpusDirectory() -> URL {
  URL(fileURLWithPath: #filePath).deletingLastPathComponent().deletingLastPathComponent().deletingLastPathComponent().deletingLastPathComponent().appendingPathComponent("core/tests/corpus")
}

/// Runs the inputs of the `corpus` machine through the public API of `Database`.
private final class DatabaseRunner {
  var database: Database?
  let mock = CorpusMock()
  let clock = TestClock(startMillis: 1_800_000_000_000)
  let heard = Heard()
  var rows: [String: String] = [:]
  var receipts: [String: WireOperationReceipt] = [:]
  var loads: [String: LoadRequest] = [:]
  var streams: [String: Pulled] = [:]
  var calls: [CallBox] = []

  func idOf(_ name: String) -> String { rows[name] ?? name }

  func nameOf(_ id: String) -> String { rows.first { $0.value == id }?.key ?? id }

  func names(_ list: [WireRow]) -> [String] { list.map { nameOf($0.id) } }

  func stateFields(_ stream: String, _ state: ReadState) -> [String: Any] {
    var out: [String: Any] = ["stream": stream, "phase": phaseName(state.phase)]
    if let error = state.error { out["error"] = "\(error)" }
    if let at = state.loaded { out["loaded"] = at }
    return out
  }

  func folded(_ stream: String, _ values: [ReadValue]) -> [String] {
    var items: [Folded] = []
    for value in values {
      switch value {
      case .delta(let base, let revision, let edits, let state):
        if case .delta(let first, _, var rows, var removed, _)? = items.last {
          apply(edits, &rows, &removed)
          items[items.count - 1] = .delta(first, revision, rows, removed, state)
        } else {
          var rows: [String] = [], removed: [String] = []
          apply(edits, &rows, &removed)
          items.append(.delta(base, revision, rows, removed, state))
        }
      case .snapshot, .count:
        items.removeAll { !$0.survivesSnapshot }
        items.append(.value(value))
      case .state:
        items.removeAll { $0.isState }
        items.append(.value(value))
      default:
        items.append(.value(value))
      }
    }
    return items.map { item in
      switch item {
      case .delta(let base, let revision, let rows, let removed, let state):
        var out = stateFields(stream, state); out["base"] = base; out["revision"] = revision; out["rows"] = rows.map(nameOf); out["removed"] = removed.map(nameOf)
        return observation("delta", out)
      case .value(let value):
        return self.value(stream, value)
      }
    }
  }

  func apply(_ edits: [RowEdit], _ rows: inout [String], _ removed: inout [String]) {
    for edit in edits {
      switch edit {
      case .insert(_, let row), .rewrite(_, let row, _):
        removed.removeAll { $0 == row.id }
        if !rows.contains(row.id) { rows.append(row.id) }
      case .remove(_, let id):
        rows.removeAll { $0 == id }
        if !removed.contains(id) { removed.append(id) }
      case .move:
        break
      }
    }
  }

  func value(_ stream: String, _ value: ReadValue) -> String {
    switch value {
    case .snapshot(let revision, let list, _, let state):
      var out = stateFields(stream, state); out["revision"] = revision; out["rows"] = names(list)
      return observation("snapshot", out)
    case .delta:
      fatalError("a delta is shown folded")
    case .count(let revision, let count, let state):
      var out = stateFields(stream, state); out["revision"] = revision; out["count"] = count
      return observation("count", out)
    case .state(_, let state):
      return observation("state", stateFields(stream, state))
    case .ready:
      return observation("ready", ["stream": stream])
    case .gap:
      return observation("gap", ["stream": stream])
    case .changes(let revision, let rows):
      return observation("changes", ["stream": stream, "revision": revision, "rows": rows.count])
    case .retry:
      return observation("unsupported", "retry")
    case .event(let position, let area, let kind, let id, _, _):
      return observation("event", ["stream": stream, "position": position, "area": area, "kind": kind, "row": nameOf(id)])
    case .lost(let first, let last):
      return observation("lost", ["stream": stream, "first": first, "last": last])
    case .status:
      return observation("unsupported", "status")
    case .operations:
      return observation("unsupported", "operations")
    case .cableClose:
      return observation("unsupported", "cable_close")
    case .ended(let reason):
      return observation("ended", ["stream": stream, "cause": causeName(reason)])
    }
  }

  func spawn(_ work: @escaping @Sendable () async -> CallResult) {
    let box = CallBox()
    calls.append(box)
    Task { box.finish(await work()) }
  }

  var wanted = 0
  var previousStart: UInt64 = 0
  var currentStart: UInt64 = 0
  var wantedFirst: String?
  var wantedOut: [[String: Any]] = []
  var at = ""
  var fence: () async -> Void { { _ = try? await self.database?.subscriptionCensus() } }

  func available() -> Int { mock.unannounced + calls.reduce(0) { $0 + ($1.done?.observed.count ?? 0) } }

  func settle(already: Int = 0) async -> [String] {
    await eventually("\(wanted - already) observations of \(at)") { available() >= wanted - already }
    await fence()
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

  func open(_ models: String, encrypted: Bool) async -> [String] {
    do {
      let session = encrypted ? EncryptedConfig(origin: SealedServer.origin, serverKey: SealedServer.serverKey, pin: SealedServer.pin, endpoints: EncryptedEndpoints(graphql: "/api/graphql", cable: "/api/cable", logout: "/api/logout", bootstrap: "/api/bootstrap", sessionOperations: ["otp": EncryptedSessionOperation(path: "/api/auth/otp", class: .authenticated), "confirm": EncryptedSessionOperation(path: "/api/auth/confirm", class: .authenticated), "social": EncryptedSessionOperation(path: "/api/auth/social", class: .authenticated), "ticket": EncryptedSessionOperation(path: "/api/ticket", class: .sessionless)]), protocolLabel: "example-ws-v1", cablePurpose: "ws.hello", cableChannel: "EncryptedGraphqlChannel") : nil
      let opened = try await OpenDatabase().openWithClock(config: Config(path: nil, dataVersion: nil), modelsJson: withRawCalls(try modelsText(models == "events" ? "models_events.json" : "models.json"), try (JSONSerialization.jsonObject(with: Data(contentsOf: corpusDirectory().appendingPathComponent("operations.json"))) as! [[String: Any]]).map { $0["name"] as! String }), ports: OpenPorts(transport: mock, encrypted: session, credentials: encrypted ? Credentials() : nil, http: encrypted ? mock : nil, endpoint: scriptedEndpoint), clock: clock, options: OpenOptions(operations: []), observers: nil)
      let operations = try String(contentsOf: corpusDirectory().appendingPathComponent("operations.json"), encoding: .utf8)
      try await registerStandOperations(opened, defsJson: operations)
      try opened.setOnline(online: true)
      database = opened
      _ = await settle(already: wanted)
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
      let request = WireUploadRequest(source: .file(path: path.path, name: "clip.bin", mime: "application/octet-stream"), variablesJson: "{}")
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
        do { return done("outcome", ["receipt": label, "result": await outcomeName(database, receipt)]) } catch { return failed(error) }
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
    case "run":
      guard let operation = text("operation") else { return failure("arguments") }
      let variables = args["variables"].map(canonical) ?? "{}"
      spawn {
        do {
          let answer = try await database.call(epoch: epoch, operation: operation, variablesJson: variables)
          let parsed = (try? JSONSerialization.jsonObject(with: Data(answer.utf8), options: [.fragmentsAllowed])) ?? NSNull()
          return done("ran", ["operation": operation, "answer": parsed])
        } catch { return failed(error) }
      }
    case "nodes":
      guard let relation = text("relation"), let names = args["rows"] as? [String] else { return failure("arguments") }
      let model = textOr(args, "model", "Moment")
      let ids = names.map(idOf)
      let byId = Dictionary(zip(ids, names), uniquingKeysWith: { first, _ in first })
      spawn {
        do {
          let found = try await database.nodes(epoch: epoch, model: model, relation: relation, rowIds: ids)
          return done("nodes", ["relation": relation, "rows": found.map { byId[$0.id] ?? $0.id }])
        } catch { return failed(error) }
      }
    case "begin_load_all":
      guard let label = text("as"), let relation = text("relation") else { return failure("arguments") }
      do {
        let load = try database.beginLoadAll(epoch: epoch, model: "Moment", relation: relation, paramsJson: "{}", force: args["force"] as? Bool ?? false)
        loads[label] = load
        spawn {
          do { try await load.result(); return done("loaded_all", ["load": label]) } catch { return failed(error) }
        }
      } catch { return failure(kind(of: error as? DbError ?? .Invalid(detail: ""))) }
    case "peek_row":
      guard let row = text("row") else { return failure("arguments") }
      do { return [observation("peeked", ["row": row, "present": try database.peekRow(epoch: epoch, model: textOr(args, "model", "Moment"), rowId: idOf(row)) != nil])] } catch { return failure(kind(of: error as? DbError ?? .Invalid(detail: ""))) }
    case "edge_values":
      guard let relation = text("relation"), let names = args["rows"] as? [String] else { return failure("arguments") }
      let model = textOr(args, "model", "Moment")
      let ids = names.map(idOf)
      let byId = Dictionary(zip(ids, names), uniquingKeysWith: { first, _ in first })
      spawn {
        do {
          let values = try await database.edgeValues(epoch: epoch, model: model, relation: relation, paramsJson: "{}", rowIds: ids)
          let named = try values.map { (byId[$0.id] ?? $0.id, try Cell.keyed(entries: $0.values).asJSON.jsonData()) }
          let parsed = Dictionary(uniqueKeysWithValues: named.map { ($0.0, (try? JSONSerialization.jsonObject(with: $0.1)) ?? [String: Any]()) })
          return done("edges", ["relation": relation, "values": parsed])
        } catch { return failed(error) }
      }
    case "upload_batch":
      guard let label = text("as"), let count = (args["count"] as? NSNumber)?.intValue, let operation = text("operation") else { return failure("arguments") }
      let path = FileManager.default.temporaryDirectory.appendingPathComponent("corpus_batch_\(ProcessInfo.processInfo.processIdentifier).bin")
      guard (try? Data("corpus".utf8).write(to: path)) != nil else { return failure("file") }
      let items = (0..<count).map { $0 + 1 }.map { WireUploadItem(valuesJson: canonical(["text": "\(label)\($0)"]), source: .file(path: path.path, name: "clip.bin", mime: "application/octet-stream"), variablesJson: "{}") }
      spawn {
        do {
          let receipt = try await database.uploadCreateBatch(epoch: epoch, model: "Moment", items: items, request: WireUploadBatch(operation: operation, variablesJson: "{}"))
          return CallResult(observed: receipt.items.indices.map { observation("receipt", ["row": "\(label)\($0 + 1)", "op": "upload"]) })
        } catch { return failed(error) }
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
        do { let found = try await database.readQuery(epoch: epoch, query: QuerySpec(model: "Moment", relation: nil, paramsJson: "{}", conditions: [], order: [], limit: nil, offset: nil, refreshOnConnect: false)); return done("row", ["row": row, "present": found.contains { $0.id == id }]) } catch { return failed(error) }
      }
    default:
      return nil
    }
    return []
  }

  func openStream(_ database: Database, _ name: String, _ args: [String: Any]) -> [String] {
    guard let label = args["as"] as? String else { return failure("arguments") }
    func query(_ relation: String?) -> QuerySpec {
      QuerySpec(model: "Moment", relation: relation, paramsJson: "{}", conditions: [], order: [], limit: nil, offset: nil, refreshOnConnect: false)
    }
    let request: ReadRequest
    switch name {
    case "open_list": request = .list(query: query(args["relation"] as? String))
    case "open_count": request = .count(query: query(args["relation"] as? String))
    case "open_entity":
      guard let row = args["row"] as? String, let fetch = (args["fetch"] as? String).flatMap(fetchOf) else { return failure("arguments") }
      request = .entity(model: "Moment", id: idOf(row), fetch: fetch)
    default:
      guard let model = args["model"] as? String else { return failure("arguments") }
      request = .events(model: model)
    }
    streams[label] = Pulled(ValueStream(epoch: database.sessionEpoch(), reader: database.subscribeValues(epoch: database.sessionEpoch(), request: request)), heard: heard)
    return []
  }

  func apply(_ input: [String: Any]) async -> [String] {
    let name = input["in"] as? String ?? ""
    let args = input["args"] as? [String: Any] ?? [:]
    previousStart = currentStart
    currentStart = clock.armed()
    switch name {
    case "open": return await open(args["models"] as? String ?? "", encrypted: args["encrypted"] as? Bool ?? false)
    case "tick":
      let ms = (args["ms"] as? NSNumber)?.uint64Value ?? 0
      if ms > 0, wanted > 0, clock.armed() <= previousStart {
        let timer = clock
        let after = previousStart
        await withTaskGroup(of: Void.self) { group in
          group.addTask { await timer.awaitArmed(after: after) }
          group.addTask { try? await expiry(hangGuard) }
          await group.next()
          group.cancelAll()
        }
      }
      clock.advance(millis: ms)
      return await settle()
    case "answer", "error", "drop", "ready", "frame":
      guard let id = args["id"] as? String else { return failure("arguments") }
      if wantedFirst != "error" { await eventually("the request \(id) of \(at)") { mock.knows(id) } }
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
        do {
          let text = canonical(event)
          reached = try mock.frame(id, text)
          if !reached, wantedFirst != "error" { await eventually("the frame \(id) of \(at)") { reached = (try? mock.frame(id, text)) ?? false; return reached } }
        } catch { return failure("frame") }
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
      guard let pulled = streams[label] else { return failure("unknown_stream") }
      let wantedHere = wantedOut.filter { ($0["v"] as? [String: Any])?["stream"] as? String == label && $0["o"] as? String != "empty" }.count
      if wantedHere > 0 { await eventually("\(wantedHere) values of the stream \(label) at \(at)") { folded(label, pulled.peek()).count >= wantedHere } }
      let unread = pulled.drain()
      observed = unread.isEmpty ? [observation("empty", ["stream": label])] : folded(label, unread)
    case "cancel":
      guard let target = args["target"] as? String else { return failure("arguments") }
      if let load = loads[target] { load.cancel() } else if let pulled = streams[target] { let stream = pulled.stream; spawn { await stream.cancel(); return CallResult() } } else { return failure("unknown_target") }
      observed = []
    default:
      guard let immediate = start(database, name, args) else { return [observation("unsupported", name)] }
      observed = immediate
    }
    observed += await settle(already: observed.count)
    if name == "close" && calls.isEmpty { self.database = nil }
    return observed
  }
}

private func corpusFiles() throws -> [URL] {
  let files = try FileManager.default.contentsOfDirectory(at: corpusDirectory(), includingPropertiesForKeys: nil).filter { $0.pathExtension == "jsonl" }.sorted { $0.lastPathComponent < $1.lastPathComponent }
  precondition(!files.isEmpty)
  return files
}

private struct CorpusStepInvalid: Error {}

private func runCorpus(_ file: URL) async throws -> String? {
  let lines = try String(contentsOf: file, encoding: .utf8).split(separator: "\n").filter { !$0.allSatisfy(\.isWhitespace) }
  let runner = DatabaseRunner()
  var mismatch: String?
  for (index, line) in lines.dropFirst().enumerated() {
    guard let step = try JSONSerialization.jsonObject(with: Data(line.utf8)) as? [String: Any] else { throw CorpusStepInvalid() }
    runner.at = "\(file.lastPathComponent) step \(index + 1) input \(canonical(step["in"] ?? ""))"
    runner.wanted = (step["out"] as? [Any])?.count ?? 0
    runner.wantedOut = step["out"] as? [[String: Any]] ?? []
    runner.wantedFirst = ((step["out"] as? [[String: Any]])?.first)?["o"] as? String
    let got = "[" + (await runner.apply(step)).joined(separator: ",") + "]"
    let expected = canonical(step["out"] ?? [])
    if got != expected {
      mismatch = "\(file.lastPathComponent) step \(index + 1) input \(canonical(step["in"] ?? "")) expected \(expected) got \(got)"
      break
    }
  }
  runner.mock.releaseAll()
  if let database = runner.database { try? await database.closeDatabase() }
  return mismatch
}

@Suite
struct CorpusRunnerTests {
  @Test(.timeLimit(.minutes(1)), arguments: try! corpusFiles())
  func bridge_prop_one_history_theSharedCorpusIsGreenThroughTheSwiftWrapper(file: URL) async throws {
    let mismatch = try await runCorpus(file)
    #expect(mismatch == nil, "\(mismatch ?? "")")
  }
}
