import Foundation
import Testing
import DBlayer

@Suite struct GeneratedModelsCompileTests {
  @Test func generatedModelsCompileAgainstThePublicInterface() {
    #expect(Asset.modelName == "Asset")
  }
}
