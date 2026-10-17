import Foundation

extension GraphqlExtensions: Codable {
  private enum CodingKeys: String, CodingKey { case code, transactionDisposition }

  public init(from decoder: any Decoder) throws {
    let fields = try decoder.container(keyedBy: CodingKeys.self)
    self.init(
      code: try fields.decodeIfPresent(String.self, forKey: .code),
      transactionDisposition: try fields.decodeIfPresent(TransactionDisposition.self, forKey: .transactionDisposition)
    )
  }

  public func encode(to encoder: any Encoder) throws {
    var fields = encoder.container(keyedBy: CodingKeys.self)
    try fields.encodeIfPresent(code, forKey: .code)
    try fields.encodeIfPresent(transactionDisposition, forKey: .transactionDisposition)
  }
}

extension TransactionDisposition: Codable {
  public init(from decoder: any Decoder) throws {
    self = transactionDisposition(value: try decoder.singleValueContainer().decode(String.self))
  }

  public func encode(to encoder: any Encoder) throws {
    var field = encoder.singleValueContainer()
    try field.encode(transactionDispositionText(disposition: self))
  }
}
