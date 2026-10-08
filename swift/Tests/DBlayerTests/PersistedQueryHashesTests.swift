import Foundation
import Testing
@testable import DBlayer

@Suite(.serialized)
struct PersistedQueryHashesTests {
  @Test
  func hashMatchesSha256OfDocumentText() async {
    #expect(
      PersistedQueryHashes.makeHash("query Me { me { id } }")
        == "2e3e49f19828396c35f7f9415f5558239d37f5d85b674851f58bf43337f5aaab"
    )
    #expect(
      PersistedQueryHashes.makeHash("mutation SignIn($token: String!) { signIn(token: $token) { id } }")
        == "16bf39dc85e5e826c9ce3776dce70ab28473b01376b4b3bbe851b4559396ba3b"
    )

    let hashes = PersistedQueryHashes()
    let first = await hashes.hash(for: "query Me { me { id } }")
    let second = await hashes.hash(for: "query Me { me { id } }")
    #expect(first == "2e3e49f19828396c35f7f9415f5558239d37f5d85b674851f58bf43337f5aaab")
    #expect(second == first)
    let mutation = await hashes.hash(for: "mutation SignIn($token: String!) { signIn(token: $token) { id } }")
    #expect(mutation == "16bf39dc85e5e826c9ce3776dce70ab28473b01376b4b3bbe851b4559396ba3b")
  }
}
