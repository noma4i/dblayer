import DBlayerTesting
import Foundation
import Testing
@testable import DBlayer

private func canonical(_ value: Any) -> String {
  guard JSONSerialization.isValidJSONObject(value), let data = try? JSONSerialization.data(withJSONObject: value, options: [.sortedKeys]) else { return "\(value)" }
  return String(decoding: data, as: UTF8.self)
}

private func observation(_ kind: String, _ value: Any? = nil) -> [String: Any] {
  guard let value else { return ["o": kind] }
  return ["o": kind, "v": value]
}

private func failure(_ kind: String) -> [[String: Any]] { [observation("error", ["kind": kind])] }

/// Runs the inputs of `specs/machines/corpus.md` (database runner) through the public API of `Database`.
/// `open`, `create`, `has`, `set_online`, `close` run; every other input answers `unsupported`.
private struct DatabaseRunner {
  var database: Database?
  var rows: [String: String] = [:]

  mutating func apply(_ input: [String: Any]) async -> [[String: Any]] {
    let name = input["in"] as? String ?? ""
    let args = input["args"] as? [String: Any] ?? [:]
    if name == "open" {
      do {
        let opened = try await openDatabase(config: Config(path: nil, dataVersion: nil, cacheRows: 32), modelsJson: modelsJSON, transport: MockTransport())
        try opened.setOnline(online: false)
        database = opened
        return [observation("opened")]
      } catch { return failure("open") }
    }
    guard let database else { return failure("not_open") }
    switch name {
    case "set_online":
      let online = args["online"] as? Bool ?? false
      do { try database.setOnline(online: online); return [observation("online", online)] } catch { return failure("online") }
    case "create":
      guard let row = args["as"] as? String, let text = args["text"] as? String else { return failure("arguments") }
      let values = canonical(["text": text])
      do {
        let receipt = try await database.send(request: CommandRequest(epoch: database.sessionEpoch(), model: "Moment", valuesJson: values, argsJson: "{}", upload: nil))
        guard let created = receipt.optimisticValue else { return failure("no_row") }
        rows[row] = created.id
        return [observation("created", ["row": row])]
      } catch { return failure("create") }
    case "has":
      guard let row = args["row"] as? String else { return failure("arguments") }
      do {
        let found = try await database.readQuery(epoch: database.sessionEpoch(), queryJson: #"{"model":"Moment"}"#)
        return [observation("row", ["row": row, "present": found.contains { $0.id == (rows[row] ?? row) }])]
      } catch { return failure("has") }
    case "close":
      do { try await database.closeDatabase(); self.database = nil; return [observation("closed")] } catch { return failure("close") }
    default:
      return [observation("unsupported", name)]
    }
  }
}

private func corpusFiles() throws -> [URL] {
  let root = URL(fileURLWithPath: #filePath).deletingLastPathComponent().deletingLastPathComponent().deletingLastPathComponent().deletingLastPathComponent()
  let directory = root.appendingPathComponent("core/tests/corpus")
  return try FileManager.default.contentsOfDirectory(at: directory, includingPropertiesForKeys: nil).filter { $0.pathExtension == "jsonl" }.sorted { $0.lastPathComponent < $1.lastPathComponent }
}

@Suite(.serialized)
struct CorpusRunnerTests {
  @Test(.timeLimit(.minutes(1)))
  func theSharedCorpusIsGreenThroughTheSwiftWrapper() async throws {
    let files = try corpusFiles()
    #expect(!files.isEmpty)
    for file in files {
      let lines = try String(contentsOf: file, encoding: .utf8).split(separator: "\n").filter { !$0.allSatisfy(\.isWhitespace) }
      var runner = DatabaseRunner()
      for (index, line) in lines.dropFirst().enumerated() {
        let step = try #require(try JSONSerialization.jsonObject(with: Data(line.utf8)) as? [String: Any])
        let got = canonical(await runner.apply(step))
        let expected = canonical(step["out"] ?? [])
        #expect(got == expected, "\(file.lastPathComponent) step \(index + 1) input \(canonical(step["in"] ?? "")) expected \(expected) got \(got)")
      }
    }
  }
}
