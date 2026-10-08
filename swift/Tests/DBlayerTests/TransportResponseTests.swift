@testable import DBlayer
import Foundation
import Testing

@Suite(.serialized)
struct TransportResponseTests {
  @Test
  func decodesSignatureCode() throws {
    let body = Data(#"{"message":"x","extensions":{"code":"SIGNATURE_EXPIRED"}}"#.utf8)
    let error = try JSONDecoder().decode(GraphQLError.self, from: body)

    #expect(error.code == "SIGNATURE_EXPIRED")
  }
}
