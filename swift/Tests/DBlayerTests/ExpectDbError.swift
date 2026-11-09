import Testing
@testable import DBlayer

nonisolated(nonsending) func expectDbError<T>(_ kind: String, sourceLocation: SourceLocation = #_sourceLocation, _ body: () async throws -> T) async {
  do {
    try await body()
    Issue.record("no error, expected \(kind)", sourceLocation: sourceLocation)
  } catch let error as DbError {
    let name = String(String(describing: error).prefix { $0 != "(" })
    #expect(name == kind, "\(error)", sourceLocation: sourceLocation)
  } catch {
    Issue.record("not a DbError: \(error)", sourceLocation: sourceLocation)
  }
}

func expectDbErrorNow<T>(_ kind: String, sourceLocation: SourceLocation = #_sourceLocation, _ body: () throws -> T) {
  do {
    _ = try body()
    Issue.record("no error, expected \(kind)", sourceLocation: sourceLocation)
  } catch let error as DbError {
    let name = String(String(describing: error).prefix { $0 != "(" })
    #expect(name == kind, "\(error)", sourceLocation: sourceLocation)
  } catch {
    Issue.record("not a DbError: \(error)", sourceLocation: sourceLocation)
  }
}
