import DBlayer
import Foundation
import Testing
@testable import DBlayer

@Suite(.serialized)
struct GraphQLResponseBodyTests {
  @Test
  func readsDataAndPersistedQueryMiss() throws {
    let text = #"{"data":{"me":{"id":"7"}}}"#
    let response = try #require(GraphQLResponseBody.parse(Data(text.utf8)))

    #expect(response.errors.isEmpty)
    #expect(
      response.data
        == JSONValue.object(["me": .object(["id": .string("7")])])
    )

    let missText = #"{"errors":[{"message":"PersistedQueryNotFound"}]}"#
    let miss = try #require(GraphQLResponseBody.parse(Data(missText.utf8)))
    let error = try #require(miss.errors.first)

    #expect(miss.errors.count == 1)
    #expect(error.message == "PersistedQueryNotFound")
    #expect(error.code == nil)
    #expect(error.status == nil)
    #expect(miss.data == nil)
    #expect(
      TransportRetryPolicy.reason(
        forErrorMessage: error.message,
        code: error.code
      ) == TransportRetryReason.persistedQueryNotFound
    )
  }

  @Test
  func readsCodeAndStatusFromExtensions() throws {
    let signatureExpiredText = #"{"errors":[{"message":"failed","extensions":{"code":"SIGNATURE_EXPIRED","http":{"status":403}}}]}"#
    let signatureExpired = try #require(
      GraphQLResponseBody.parse(Data(signatureExpiredText.utf8))
    )
    let signatureExpiredError = try #require(signatureExpired.errors.first)
    #expect(signatureExpiredError.code == "SIGNATURE_EXPIRED")
    #expect(signatureExpiredError.status == 403)

    let unauthenticatedText = #"{"errors":[{"message":"failed","extensions":{"code":"UNAUTHENTICATED","status":401}}]}"#
    let unauthenticated = try #require(
      GraphQLResponseBody.parse(Data(unauthenticatedText.utf8))
    )
    let unauthenticatedError = try #require(unauthenticated.errors.first)
    #expect(unauthenticatedError.code == "UNAUTHENTICATED")
    #expect(unauthenticatedError.status == 401)

    let numericCodeText = #"{"errors":[{"message":"failed","extensions":{"code":5}}]}"#
    let numericCode = try #require(
      GraphQLResponseBody.parse(Data(numericCodeText.utf8))
    )
    let numericCodeError = try #require(numericCode.errors.first)
    #expect(numericCodeError.code == nil)
  }

  @Test
  func rejectsBrokenBody() throws {
    let invalidJSON = Data("not json".utf8)
    #expect(GraphQLResponseBody.parse(invalidJSON) == nil)

    let arrayBody = Data("[1,2]".utf8)
    #expect(GraphQLResponseBody.parse(arrayBody) == nil)

    let missingMessageText = #"{"errors":[{"path":["me"]},{"message":"ok"}]}"#
    let response = try #require(
      GraphQLResponseBody.parse(Data(missingMessageText.utf8))
    )
    let error = try #require(response.errors.first)
    #expect(response.errors.count == 1)
    #expect(error.message == "ok")
  }
}
