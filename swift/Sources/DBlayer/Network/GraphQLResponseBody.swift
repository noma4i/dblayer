
import Foundation

public struct GraphQLResponseError: Equatable, Sendable {
  public let message: String
  public let code: String?
  public let status: Int?
}

public struct GraphQLResponseBody: Equatable, Sendable {
  public let data: JSONValue?
  public let errors: [GraphQLResponseError]

  public static func parse(_ body: Data) -> GraphQLResponseBody? {
    guard let payload = try? JSONDecoder().decode(Payload.self, from: body) else {
      return nil
    }

    let errors = (payload.errors ?? []).compactMap { error -> GraphQLResponseError? in
      guard let message = error.message else {
        return nil
      }
      return GraphQLResponseError(
        message: message,
        code: error.extensions?.code,
        status: error.extensions?.http?.status ?? error.extensions?.status
      )
    }

    return GraphQLResponseBody(data: payload.data, errors: errors)
  }
}

private struct Payload: Decodable {
  let data: JSONValue?
  let errors: [ResponseError]?
}

private struct ResponseError: Decodable {
  let message: String?
  let extensions: ResponseExtensions?

  private enum CodingKeys: String, CodingKey {
    case message
    case extensions
  }

  init(from decoder: Decoder) throws {
    guard let container = try? decoder.container(keyedBy: CodingKeys.self) else {
      message = nil
      extensions = nil
      return
    }
    message = try? container.decode(String.self, forKey: .message)
    extensions = try? container.decode(ResponseExtensions.self, forKey: .extensions)
  }
}

private struct ResponseExtensions: Decodable {
  let code: String?
  let status: Int?
  let http: ResponseHTTP?

  private enum CodingKeys: String, CodingKey {
    case code
    case status
    case http
  }

  init(from decoder: Decoder) throws {
    let container = try decoder.container(keyedBy: CodingKeys.self)
    code = try? container.decode(String.self, forKey: .code)
    status = try? container.decode(Int.self, forKey: .status)
    http = try? container.decode(ResponseHTTP.self, forKey: .http)
  }
}

private struct ResponseHTTP: Decodable {
  let status: Int?

  private enum CodingKeys: String, CodingKey {
    case status
  }

  init(from decoder: Decoder) throws {
    let container = try decoder.container(keyedBy: CodingKeys.self)
    status = try? container.decode(Int.self, forKey: .status)
  }
}
