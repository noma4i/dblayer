import DBlayer
import Foundation
import Testing
@testable import DBlayer

@Suite(.serialized)
struct GraphQLOperationTests {
  @Test
  func publicOperationsMatchV2() {
    #expect(PublicOperations.names == ["AuthSignIn", "AuthConsumeLoginLink"])

    let publicRequest = GraphQLRequest(
      operationName: "AuthSignIn",
      document: "",
      variables: [String: String]()
    )
    let privateRequest = GraphQLRequest(
      operationName: "myProfile",
      document: "",
      variables: [String: String]()
    )

    #expect(publicRequest.isPublic == true)
    #expect(privateRequest.isPublic == false)
  }
}
