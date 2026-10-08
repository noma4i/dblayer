import Testing
@testable import DBlayer

@Suite(.serialized)
struct SubscriptionActivationTests {
  @Test
  func isActiveOnlyWhenAllConditionsMet() {
    #expect(SubscriptionActivation.isActive(Self.state()) == true)
    #expect(SubscriptionActivation.isActive(Self.state(preload: false)) == false)
    #expect(SubscriptionActivation.isActive(Self.state(authenticated: false)) == false)
    #expect(SubscriptionActivation.isActive(Self.state(pendingAuthRoute: true)) == false)
    #expect(SubscriptionActivation.isActive(Self.state(registered: false)) == false)
  }

  @Test
  func blockingReasonNamesEarliestFailure() {
    #expect(SubscriptionActivation.blockingReason(Self.state()) == nil)
    #expect(SubscriptionActivation.blockingReason(Self.state(preload: false, authenticated: false)) == "preload")
    #expect(SubscriptionActivation.blockingReason(Self.state(pendingAuthRoute: true)) == "auth-route")
    #expect(SubscriptionActivation.blockingReason(Self.state(registered: false)) == "registration")
  }

  @Test
  func transitionAndDocumentsToOpenMatchSpec() {
    #expect(SubscriptionActivation.transition(from: false, to: true) == "subscribe")
    #expect(SubscriptionActivation.transition(from: true, to: false) == "unsubscribe")
    #expect(SubscriptionActivation.transition(from: true, to: true) == "keep")
    #expect(SubscriptionActivation.transition(from: false, to: false) == "keep")
    #expect(SubscriptionActivation.documentsToOpen(state: Self.state(), catalog: ["a", "b"]) == ["a", "b"])
    #expect(SubscriptionActivation.documentsToOpen(state: Self.state(preload: false), catalog: ["a"]).isEmpty)
  }

  private static func state(
    preload: Bool = true,
    authenticated: Bool = true,
    pendingAuthRoute: Bool = false,
    registered: Bool = true
  ) -> SessionState {
    SessionState(
      preloadFinished: preload,
      isAuthenticated: authenticated,
      hasPendingAuthRoute: pendingAuthRoute,
      registrationCompleted: registered
    )
  }
}
