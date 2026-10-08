import Testing
@testable import DBlayer

@Suite(.serialized)
struct UnauthorizedDecisionTests {
  @Test
  func logsOutOnlyForMyProfileWithUnauthorizedSignal() {
    #expect(UnauthorizedDecision.logsOut(operationName: "myProfile", status: 401, codes: [], messages: []))
    #expect(!UnauthorizedDecision.logsOut(operationName: "sendMessage", status: 401, codes: [], messages: []))
    #expect(
      UnauthorizedDecision.logsOut(operationName: "myProfile", status: nil, codes: ["UNAUTHENTICATED"], messages: [])
    )
    #expect(
      UnauthorizedDecision.logsOut(
        operationName: "myProfile",
        status: nil,
        codes: [],
        messages: ["Request is Unauthorized"]
      )
    )
    #expect(
      !UnauthorizedDecision.logsOut(operationName: "myProfile", status: nil, codes: [], messages: ["Server error"])
    )
  }

  @Test
  func reportsFatalAndIsSecurityCodeMatchSpec() {
    #expect(UnauthorizedDecision.reportsFatal(status: 502) == true)
    #expect(UnauthorizedDecision.reportsFatal(status: 404) == false)
    #expect(UnauthorizedDecision.reportsFatal(status: nil) == false)

    #expect(UnauthorizedDecision.isSecurityCode("SIGNATURE_INVALID") == true)
    #expect(UnauthorizedDecision.isSecurityCode("TIMEOUT") == false)
  }
}
