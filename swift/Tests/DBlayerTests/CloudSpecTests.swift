import Foundation
import Testing
@testable import DBlayer

struct CloudSpecTests {
  @Test
  func modelsJsonCarriesTheExpandedCloudList() throws {
    let models = try #require(JSONSerialization.jsonObject(with: Data(modelsJSON.utf8)) as? [[String: Any]])
    func cloud(_ key: String) -> [String]? {
      models.first { $0["key"] as? String == key }?["cloud"] as? [String]
    }
    #expect(cloud("CloudDraft") == ["title", "pinned"])
    #expect(cloud("CloudCursor") == ["chatId", "readUntil"])
    #expect(cloud("CloudNote") == ["tone"])
    #expect(models.first { $0["key"] as? String == "StorageRow" }?["cloud"] == nil)
  }
}
