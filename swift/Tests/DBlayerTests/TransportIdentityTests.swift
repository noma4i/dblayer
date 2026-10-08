import Foundation
import Testing
@testable import DBlayer

@Suite(.serialized)
struct TransportIdentityTests {
  @Test
  func identityIsTokenForPrivateAndDeviceForPublicOperations() throws {
    let privateIdentity = try TransportIdentity.resolve(
      operationName: "myProfile",
      token: "tok",
      deviceUUID: "device-1"
    )
    let signInIdentity = try TransportIdentity.resolve(
      operationName: "AuthSignIn",
      token: nil,
      deviceUUID: "device-1"
    )
    let consumeLinkIdentity = try TransportIdentity.resolve(
      operationName: "AuthConsumeLoginLink",
      token: "tok",
      deviceUUID: "device-1"
    )
    #expect(privateIdentity == "tok")
    #expect(signInIdentity == "device-1")
    #expect(consumeLinkIdentity == "device-1")

    for token in [nil, ""] as [String?] {
      #expect {
        try TransportIdentity.resolve(operationName: "myProfile", token: token, deviceUUID: "device-1")
      } throws: { error in
        error as? GraphQLTransportError == .unauthenticated
      }
    }
  }

  @Test
  func authorizationTokenSkipsPublicOperations() {
    #expect(TransportIdentity.authorizationToken(operationName: "AuthSignIn", token: "tok") == nil)
    #expect(TransportIdentity.authorizationToken(operationName: "myProfile", token: "tok") == "tok")
    #expect(TransportIdentity.authorizationToken(operationName: "myProfile", token: nil) == nil)
  }
}
