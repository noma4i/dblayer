import DBlayerTesting
import Foundation
import Testing
@testable import DBlayer

private let serverKey = "MIIBojANBgkqhkiG9w0BAQEFAAOCAY8AMIIBigKCAYEAtzb67l/cXul2uArFqeZXsMf4tImrLcaHb3uiVNp8mjjJrFK/zwUEEazm3pmriAbj1nBzSxQLuq3jNUeFHXoQxjwWmTdv+PoAypz4ZII6Dz6rknk290/ddmimc1Z4Mx0AkT4ffAA9WHiDtLPF6tKZWNsWF/SpJps9PWrIKNOKY78ZfCgfegrWfKwoNYExyZ0IuUUgG0lsRjEEE/U59GpbNBqdKtw7KaIGK47QRk6HWOkCQtyqqjsseOJLwdI0k6jDNmcH6DulPNmar9AbwsmpngD55P8y4JdH49Eq3Nr040gmg0Y1p1FZm5PbKeC4WgZZdmg1bxjKAa+MYP0fROi4dFz05/81fqFb6DWaXencb0p/Iz+IUfQiI/0EYNv1SfdTUHzGyHFAmQHeCUIKLSkNNg3uF/jqKthFecCnWEfTUWRWFsN/8/Nb65a1UI/L1hP/qd+T6xKfchqVBEf767yeHdgsy3GeuqTsgXUPkuA1DO9HPP1xWqa6orUNWkimSwxDAgMBAAE="
private let serverPin = "fab4f816eb33917161942684230405ae47ad4df5b456bd973085599b1364342a"
private let otherPin = String(repeating: "0", count: 64)

private func configBody(kid: String) -> String {
  """
  {"protocol_version":1,"min_app_version":"3.8.0","graphql_url":"https://api.example.com/api/v3/graphql","cable_url":"wss://api.example.com/api/v3/cable","server_key":{"kid":"\(kid)","public_key_der":"\(serverKey)","alg":"RSA-OAEP","enc":"A256GCM","bits":3072}}
  """
}

private final class ScriptedHttp: EncryptedHttp, @unchecked Sendable {
  private let lock = NSLock()
  private var recorded: [String] = []
  private var status: UInt16 = 503
  private var offline = false
  private var config = ""

  var paths: [String] { lock.withLock { recorded } }
  func answer(status: UInt16) { lock.withLock { self.status = status } }
  func goOffline(_ value: Bool) { lock.withLock { offline = value } }
  func serve(config: String) { lock.withLock { self.config = config } }

  func post(path: String, body: String) async throws -> EncryptedReply {
    let (status, offline) = lock.withLock { () -> (UInt16, Bool) in
      recorded.append(path)
      return (self.status, self.offline)
    }
    if offline { throw DbError.Network(detail: "offline") }
    return EncryptedReply(status: status, body: "")
  }

  func get(path: String) async throws -> EncryptedReply {
    EncryptedReply(status: 200, body: lock.withLock { config })
  }
}

private final class CountedCredential: CredentialStore, @unchecked Sendable {
  private let lock = NSLock()
  private var value: String?
  private var setCount = 0
  private var clearCount = 0

  init(_ value: String? = nil) { self.value = value }

  var current: String? { lock.withLock { value } }
  var touched: (sets: Int, clears: Int) { lock.withLock { (setCount, clearCount) } }
  func get() throws -> String? { current }
  func set(credential: String) throws { lock.withLock { value = credential; setCount += 1 } }
  func clear() throws { lock.withLock { value = nil; clearCount += 1 } }
}

private final class ClosingSocket: CableSocket, @unchecked Sendable {
  func open() async throws {}
  func close() {}
  func send(_ text: String) async throws {}
  func receive() async throws -> String { throw CableSocketError.closed }
}

private func open(_ http: ScriptedHttp, _ credentials: CountedCredential, path: String? = nil, pin: String = serverPin) throws -> Database {
  try Database.openEncrypted(
    config: Config(path: path, dataVersion: nil, cacheRows: 32),
    modelsJson: modelsJSON,
    encrypted: EncryptedConfig(origin: "https://api.example.com", serverKey: serverKey, pin: pin),
    http: http,
    credentials: credentials,
    transport: DatabaseTransport(cable: { nil })
  )
}

private func temporaryFile() throws -> (URL, String) {
  let directory = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
  try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
  return (directory, directory.appendingPathComponent("v3.sqlite").path)
}

@Suite(.serialized)
struct SessionGuardsTests {
  @Test
  func aPinMismatchAtOpenAndInTheConfigNeverTouchesTheCredentialStore() async throws {
    let http = ScriptedHttp()
    let credentials = CountedCredential("kept")
    #expect(throws: DbError.self) { try open(http, credentials, pin: otherPin) }
    let database = try open(http, credentials)
    http.serve(config: configBody(kid: otherPin))

    do {
      _ = try await database.serverConfig()
      Issue.record("expected a key mismatch")
    } catch DbError.Session(let code, _) {
      #expect(code == .serverKeyMismatch)
    }

    #expect(credentials.current == "kept")
    #expect(credentials.touched == (0, 0))
  }

  @Test
  func httpRefusalsKeepTheCredentialAndTheRequestStaysPendingForResume() async throws {
    for status: UInt16 in [401, 403, 423, 451] {
      let http = ScriptedHttp()
      let credentials = CountedCredential()
      let database = try open(http, credentials)
      http.answer(status: status)
      await #expect(throws: DbError.self) { try await database.sessionCall(operation: "otp", bodyJson: #"{"phone":"+61400000000"}"#) }
      let issued = credentials.current

      await #expect(throws: DbError.self) { try await database.sessionResume() }

      #expect(http.paths == ["/api/v3/auth/otp", "/api/v3/auth/otp"], Comment(rawValue: "\(status)"))
      #expect(credentials.current == issued && credentials.touched == (1, 0), Comment(rawValue: "\(status)"))
    }
  }

  @Test
  func resumeAfterARestartSendsThePendingRequestAndNothingWithoutOne() async throws {
    let (directory, path) = try temporaryFile()
    defer { try? FileManager.default.removeItem(at: directory) }
    let http = ScriptedHttp()
    let credentials = CountedCredential()
    var database = try open(http, credentials, path: path)
    http.goOffline(true)
    await #expect(throws: DbError.self) { try await database.sessionCall(operation: "otp", bodyJson: #"{"phone":"+61400000000"}"#) }
    http.goOffline(false)

    database = try open(http, credentials, path: path)
    await #expect(throws: DbError.self) { try await database.sessionResume() }
    #expect(http.paths == ["/api/v3/auth/otp", "/api/v3/auth/otp"])

    let (fresh, freshPath) = try temporaryFile()
    defer { try? FileManager.default.removeItem(at: fresh) }
    let quiet = ScriptedHttp()
    let nothing = try open(quiet, CountedCredential(), path: freshPath)
    #expect(try await nothing.sessionResume() == nil)
    #expect(quiet.paths.isEmpty)
  }

  @Test
  func aLogoutWhoseAnswerWasLostIsResentAfterARestartAndKeepsTheCredentialUntilItIsRevoked() async throws {
    let (directory, path) = try temporaryFile()
    defer { try? FileManager.default.removeItem(at: directory) }
    let http = ScriptedHttp()
    let credentials = CountedCredential()
    var database = try open(http, credentials, path: path)
    http.goOffline(true)
    await #expect(throws: DbError.self) { try await database.sessionCall(operation: "logout", bodyJson: "{}") }
    http.goOffline(false)

    database = try open(http, credentials, path: path)
    await #expect(throws: DbError.self) { try await database.sessionResume() }

    #expect(http.paths == ["/api/v3/logout", "/api/v3/logout"])
    #expect(credentials.current?.count == 43 && credentials.touched == (1, 0))
  }

  @Test
  func aCredentialSavedBeforeItsSessionRowIsReplacedByTheNextCall() async throws {
    let http = ScriptedHttp()
    let credentials = CountedCredential("saved-before-the-crash")
    let database = try open(http, credentials)

    await #expect(throws: DbError.self) { try await database.sessionCall(operation: "otp", bodyJson: #"{"phone":"+61400000000"}"#) }

    #expect(credentials.current != "saved-before-the-crash" && credentials.current?.count == 43)
    #expect(credentials.touched == (1, 0))
  }

  @Test
  func aClosedCableLeavesTheCredentialInTheAppStore() async throws {
    let http = ScriptedHttp()
    let credentials = CountedCredential()
    try DB.openEncrypted(
      encrypted: EncryptedConfig(origin: "https://api.example.com", serverKey: serverKey, pin: serverPin),
      credentials: credentials,
      transport: DatabaseTransport(cable: { nil }),
      http: http,
      modelsJSON: modelsJSON
    )
    await #expect(throws: DbError.self) { try await DB.database.sessionCall(operation: "otp", bodyJson: #"{"phone":"+61400000000"}"#) }
    let issued = credentials.current
    let connection = CableConnection(url: URL(fileURLWithPath: "/test"), token: "token", makeSocket: { _, _ in ClosingSocket() })

    await connection.open()
    await eventually("the closed cable") { await connection.state == .closed }

    #expect(credentials.current == issued && credentials.touched == (1, 0))
  }
}
