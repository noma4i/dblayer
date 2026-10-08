import Foundation

public struct RequestFailure: Equatable, Sendable {
  public let operationName: String
  public let status: Int?
  public let errorClass: String

  public init(operationName: String, status: Int?, errorClass: String) {
    self.operationName = operationName
    self.status = status
    self.errorClass = errorClass
  }
}
