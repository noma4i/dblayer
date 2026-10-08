import Foundation

public actor ServerClock {
  public private(set) var offset: TimeInterval = 0

  public init() {}

  public func update(fromDateHeader header: String?, receivedAt: Date) {
    guard let header, let serverDate = Self.dateFormatter.date(from: header) else {
      return
    }
    offset = serverDate.timeIntervalSince(receivedAt)
  }

  public func now(reference: Date = Date()) -> Date {
    reference.addingTimeInterval(offset)
  }
}

private extension ServerClock {
  static let dateFormatter: DateFormatter = {
    let formatter = DateFormatter()
    formatter.locale = Locale(identifier: "en_US_POSIX")
    formatter.timeZone = TimeZone(secondsFromGMT: 0)
    formatter.dateFormat = "EEE, dd MMM yyyy HH:mm:ss zzz"
    return formatter
  }()
}
