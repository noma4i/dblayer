import CloudKit
import DBlayer
import SwiftUI

let hostContainerID = "iCloud.com.yupi"

struct CloudHostReport {
  var account: String
  var push: String

  var line: String { "account=\(account) push=\(push)" }
}

func probeCloudHost() async -> CloudHostReport {
  let adapter = CloudKitAdapter(container: CKContainer(identifier: hostContainerID))
  let account: String
  do {
    switch try await adapter.account() {
    case .available: account = "available"
    case .signedOut: account = "signedOut"
    case .needsConsent: account = "needsConsent"
    case .restricted: account = "restricted"
    }
  } catch {
    account = "error(\(error))"
  }
  let probe = CloudBlob(key: "cloud-host-probe", bytes: Data([1]))
  let push: String
  do {
    try await adapter.push(blobs: [probe])
    push = "accepted"
  } catch let error as CloudError {
    switch error {
    case .NeedsAuthorization: push = "needsAuthorization"
    case .Offline: push = "offline"
    case .Throttled: push = "throttled"
    case .Unavailable: push = "unavailable"
    case .QuotaExceeded: push = "quotaExceeded"
    case .Rejected(let detail): push = "rejected(\(detail))"
    case .Interrupted(let detail): push = "interrupted(\(detail))"
    }
  } catch {
    push = "error(\(error))"
  }
  return CloudHostReport(account: account, push: push)
}

@main
struct CloudHostApp: App {
  var body: some Scene {
    WindowGroup {
      Text("CloudHost").task {
        let report = await probeCloudHost()
        print("CLOUDHOST \(report.line)")
        if ProcessInfo.processInfo.environment["CLOUDHOST_EXIT"] == "1" { exit(0) }
      }
    }
  }
}
