import Foundation

public enum GraphQLURLRequest {
  public static func make(
    url: URL,
    body: Data,
    headers: [String: String],
    timeout: TimeInterval = 30
  ) -> URLRequest {
    var request = URLRequest(url: url, timeoutInterval: timeout)
    request.httpMethod = "POST"
    request.httpBody = body
    request.setValue("application/json", forHTTPHeaderField: "Content-Type")

    for (field, value) in headers {
      request.setValue(value, forHTTPHeaderField: field)
    }

    request.setUserAgent()
    return request
  }
}

extension URLRequest {
  mutating func setUserAgent() {
    setValue(userAgent(application: value(forHTTPHeaderField: "User-Agent")), forHTTPHeaderField: "User-Agent")
  }
}
