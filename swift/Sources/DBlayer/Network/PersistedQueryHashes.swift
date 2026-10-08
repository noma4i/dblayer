import CryptoKit
import Foundation

public actor PersistedQueryHashes {
  private let hashesByDocument = NSCache<NSString, NSString>()

  public init() {}

  public static func makeHash(_ document: String) -> String {
    SHA256.hash(data: Data(document.utf8))
      .map { String(format: "%02x", $0) }
      .joined()
  }

  public func hash(for document: String) -> String {
    if let cached = hashesByDocument.object(forKey: document as NSString) {
      return cached as String
    }
    let computed = Self.makeHash(document)
    hashesByDocument.setObject(computed as NSString, forKey: document as NSString)
    return computed
  }
}
