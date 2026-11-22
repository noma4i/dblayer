import Foundation
import Testing
@testable import DBlayer

struct ICloudDocumentsTests {
  @Test
  func snapshotsIncludeUndownloadedDocumentsInTheSelectedFolder() throws {
    let documents = URL(fileURLWithPath: "/example/Container/Documents", isDirectory: true)
    let folder = documents.appendingPathComponent("Scans", isDirectory: true)
    func item(_ path: String, content: [String] = ["com.adobe.pdf", "public.data"]) -> [String: Any] {
      let url = documents.appendingPathComponent(path)
      return [
        NSMetadataItemURLKey: url,
        NSMetadataItemFSNameKey: url.lastPathComponent,
        NSMetadataItemContentTypeKey: content[0],
        NSMetadataItemContentTypeTreeKey: content,
        NSMetadataUbiquitousItemDownloadingStatusKey: NSMetadataUbiquitousItemDownloadingStatusNotDownloaded,
        NSMetadataUbiquitousItemIsUploadedKey: true,
      ]
    }

    var downloading = item("Scans/Downloading.pdf")
    downloading[NSMetadataUbiquitousItemIsDownloadingKey] = true
    downloading[NSMetadataUbiquitousItemUploadingErrorKey] = NSError(domain: NSCocoaErrorDomain, code: NSFileWriteNoPermissionError)
    let items = [
      item("Scans/Remote.pdf"), downloading,
      item("ScansBackup/Other.pdf"), item("Scans/Nested/Other.pdf"), item("Scans/.hidden.pdf"),
      item("Scans/Nested", content: ["public.folder", "public.directory"]),
      item("Scans/Link", content: ["public.symlink"]),
    ]
    let snapshot = ICloudDocument.snapshot(items: items, folder: folder)
    #expect(snapshot.map(\.name) == ["Downloading.pdf", "Remote.pdf"])
    let first = try #require(snapshot.first)
    let second = try #require(snapshot.last)
    #expect(first.downloadStatus == .downloading)
    #expect(first.uploadStatus == .failed)
    #expect(second.downloadStatus == .notDownloaded)
    #expect(second.uploadStatus == .uploaded)
  }

  @Test(.timeLimit(.minutes(1)))
  func appAndExternalFilesShareTheSameDocumentsFolder() async throws {
    let root = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString, isDirectory: true)
    let container = root.appendingPathComponent("Container", isDirectory: true)
    let folder = container.appendingPathComponent("Documents/Scans", isDirectory: true)
    try FileManager.default.createDirectory(at: container, withIntermediateDirectories: true)
    defer { try? FileManager.default.removeItem(at: root) }
    let documents = ICloudDocuments(containerURL: { container })
    let source = root.appendingPathComponent("source.pdf")
    let first = Data("from the app".utf8)
    try first.write(to: source)
    #expect(!FileManager.default.fileExists(atPath: folder.path))

    try await documents.createFolder("Scans")
    try await documents.createFolder("Scans")
    try await documents.add(source, named: "Phone.pdf", to: "Scans")
    let added = folder.appendingPathComponent("Phone.pdf")
    #expect(try Data(contentsOf: added) == first)
    #expect(try Data(contentsOf: source) == first)

    let second = Data("from Finder".utf8)
    try second.write(to: folder.appendingPathComponent("Mac.pdf"))
    let exported = root.appendingPathComponent("export.pdf")
    try await documents.copy("Mac.pdf", in: "Scans", to: exported)
    #expect(try Data(contentsOf: exported) == second)

    await #expect(throws: ICloudDocuments.Error.self) {
      try await documents.copy("Phone.pdf", in: "Scans", to: exported)
    }
    #expect(try Data(contentsOf: exported) == second)
    await #expect(throws: ICloudDocuments.Error.self) {
      try await documents.add(exported, named: "Phone.pdf", to: "Scans")
    }
    #expect(try Data(contentsOf: added) == first)
  }

  @Test(.timeLimit(.minutes(1)))
  func rejectsTraversalAndSymlinks() async throws {
    let root = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString, isDirectory: true)
    let container = root.appendingPathComponent("Container", isDirectory: true)
    try FileManager.default.createDirectory(at: container, withIntermediateDirectories: true)
    defer { try? FileManager.default.removeItem(at: root) }
    let documents = ICloudDocuments(containerURL: { container })
    for name in ["", ".", "..", "Scans/Other", "a\u{0}b"] {
      await #expect(throws: ICloudDocuments.Error.self) { try await documents.createFolder(name) }
    }

    try await documents.createFolder("Scans")
    let folder = container.appendingPathComponent("Documents/Scans", isDirectory: true)
    let outside = root.appendingPathComponent("outside.pdf")
    try Data("outside".utf8).write(to: outside)
    try FileManager.default.createSymbolicLink(at: folder.appendingPathComponent("Link.pdf"), withDestinationURL: outside)
    let exported = root.appendingPathComponent("export.pdf")
    await #expect(throws: ICloudDocuments.Error.self) {
      try await documents.copy("Link.pdf", in: "Scans", to: exported)
    }
    #expect(!FileManager.default.fileExists(atPath: exported.path))
  }
}
