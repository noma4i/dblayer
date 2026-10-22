import Foundation

extension URLRequest {
  mutating func setUserAgent() {
    setValue(userAgent(application: value(forHTTPHeaderField: "User-Agent")), forHTTPHeaderField: "User-Agent")
  }
}
