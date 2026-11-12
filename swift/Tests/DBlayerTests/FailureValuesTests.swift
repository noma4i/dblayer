import DBlayer
import DBlayerTesting
import Foundation
import Testing

struct FailureValuesTests {
  /// The functions of the testing product build a refusal, a sync value and an upload value, and each reads as the row of a failed operation does.
  @Test
  func bridge_failure_values_in_tests_swiftBuildsTheValuesFromFields() throws {
    let failure = operationFailure(kind: "Invalid", reason: "the cell is empty", message: "refused")
    #expect((failure.kind, failure.reason, failure.message, failure.sessionCode, failure.requestSize) == ("Invalid", "the cell is empty", "refused", nil, nil))
    let sync = syncInfo(state: .failed, error: failure)
    #expect((sync.state, sync.error, sync.actions, sync.attempts) == (.failed, failure, [], 0))
    let upload = uploadInfo(status: .failed, progress: 40, error: failure)
    #expect((upload.status, upload.progress, upload.error) == (.failed, 40, failure))
    #expect(try JSONDecoder().decode(OperationFailure.self, from: JSONEncoder().encode(failure)) == failure)
    #expect(try JSONDecoder().decode(SyncInfo.self, from: JSONEncoder().encode(sync)) == sync)
  }
}
