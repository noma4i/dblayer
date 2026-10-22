import XCTest
@testable import CloudHost

final class CloudHostTests: XCTestCase {
  func testASignedOutSimulatorReportsSignedOutAndNeedsAuthorization() async {
    let report = await probeCloudHost()
    XCTAssertEqual(report.line, "account=signedOut push=needsAuthorization")
  }
}
