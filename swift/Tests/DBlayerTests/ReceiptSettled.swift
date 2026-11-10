import DBlayer

/// How `confirmed()` of a receipt ended: the value, the refusal with `kept`, or the end of the wait with the session or the database.
enum Settled<Model: DBModel>: Sendable {
  case confirmed(Model?, String?)
  case rejected(WireFailure, kept: Bool)
  case ended(cause: EndCause)

  var confirmedValue: Model? {
    if case let .confirmed(value, _) = self { return value }
    return nil
  }
}

extension MutationReceipt {
  func settled() async throws -> Settled<Model> {
    do {
      let confirmed = try await confirmed()
      return .confirmed(confirmed.value, confirmed.answerJson)
    } catch let DbError.Rejected(failure, kept) {
      return .rejected(failure, kept: kept)
    } catch DbError.NoSession {
      return .ended(cause: .sessionEnded)
    } catch DbError.Closed {
      return .ended(cause: .closed)
    }
  }
}
