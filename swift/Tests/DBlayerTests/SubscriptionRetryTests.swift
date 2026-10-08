import Testing
@testable import DBlayer

@Suite(.serialized)
struct SubscriptionRetryTests {
  @Test
  func retryNeedsOnlineForegroundAndActiveSession() {
    #expect(SubscriptionRetry.retries(isOnline: true, isForeground: true))
    #expect(!SubscriptionRetry.retries(isOnline: true, isForeground: false))
    #expect(!SubscriptionRetry.retries(isOnline: false, isForeground: true))
    #expect(!SubscriptionRetry.retries(isOnline: false, isForeground: false))
    #expect(SubscriptionRetry.isActive(
      isEnabled: true,
      isAuthenticated: true,
      hasSuccessContext: false,
      registrationCompleted: true
    ))
    #expect(!SubscriptionRetry.isActive(
      isEnabled: false,
      isAuthenticated: true,
      hasSuccessContext: false,
      registrationCompleted: true
    ))
    #expect(!SubscriptionRetry.isActive(
      isEnabled: true,
      isAuthenticated: false,
      hasSuccessContext: false,
      registrationCompleted: true
    ))
    #expect(!SubscriptionRetry.isActive(
      isEnabled: true,
      isAuthenticated: true,
      hasSuccessContext: true,
      registrationCompleted: true
    ))
    #expect(SubscriptionRetry.resetsAttempts(onServerAck: true))
    #expect(!SubscriptionRetry.resetsAttempts(onServerAck: false))
  }
}
