import Foundation
import Darwin

public struct ICloudDocuments: Sendable {
  public enum Error: Swift.Error {
    case unavailable
    case accountChanged
    case invalidName
    case symbolicLink
    case alreadyExists
    case downloadRequired
    case notRegularFile
    case coordinationFailed
  }

  private let resolveContainer: @Sendable () throws -> URL
  private let captureIdentity: @Sendable () throws -> (@Sendable () throws -> Void)

  public init(containerIdentifier: String? = nil) {
    self.resolveContainer = {
      guard let url = FileManager.default.url(forUbiquityContainerIdentifier: containerIdentifier) else {
        throw Error.unavailable
      }
      return url
    }
    self.captureIdentity = {
      guard let identity = FileManager.default.ubiquityIdentityToken else { throw Error.unavailable }
      return {
        guard let current = FileManager.default.ubiquityIdentityToken else { throw Error.accountChanged }
        guard identity.isEqual(current) else { throw Error.accountChanged }
      }
    }
  }

  init(containerURL: @escaping @Sendable () throws -> URL) {
    self.resolveContainer = containerURL
    self.captureIdentity = { {} }
  }

  static func validate(_ name: String) throws {
    guard !name.isEmpty, name != ".", name != "..", !name.contains("/"), !name.contains("\0") else {
      throw Error.invalidName
    }
  }

  struct ResolvedFolder: Sendable {
    let url: URL
    let checkIdentity: @Sendable () throws -> Void
  }

  func folderURL(_ folder: String) async throws -> ResolvedFolder {
    try Self.validate(folder)
    return try await Task.detached {
      let checkIdentity = try captureIdentity()
      let root = try resolveContainer()
      let documents = root.appendingPathComponent("Documents", isDirectory: true)
      let result = documents.appendingPathComponent(folder, isDirectory: true)
      try Self.rejectSymlinks([root, documents, result])
      try checkIdentity()
      return ResolvedFolder(url: result, checkIdentity: checkIdentity)
    }.value
  }

  public func createFolder(_ name: String) async throws {
    let resolved = try await folderURL(name)
    let folder = resolved.url
    let checkIdentity = resolved.checkIdentity
    try await Task.detached {
      try Self.coordinateWriting(folder.deletingLastPathComponent()) { documents in
        let target = documents.appendingPathComponent(name, isDirectory: true)
        try checkIdentity()
        try Self.rejectSymlinks([documents.deletingLastPathComponent(), documents, target])
        try FileManager.default.createDirectory(at: target, withIntermediateDirectories: true)
      }
    }.value
  }

  public func add(_ source: URL, named name: String, to folder: String) async throws {
    try Self.validate(name)
    let resolved = try await folderURL(folder)
    let directory = resolved.url
    let checkIdentity = resolved.checkIdentity
    try await Task.detached {
      try Self.coordinateReading(source, writing: directory) { sourceURL, directoryURL in
        let destination = directoryURL.appendingPathComponent(name)
        try checkIdentity()
        try Self.rejectSymlinks([directoryURL.deletingLastPathComponent(), directoryURL, destination])
        try Self.requireRegularFile(sourceURL)
        try Self.copyStaged(sourceURL, to: destination, checkIdentity: checkIdentity)
      }
    }.value
  }

  public func download(_ name: String, in folder: String) async throws {
    try Self.validate(name)
    let resolved = try await folderURL(folder)
    let directory = resolved.url
    let checkIdentity = resolved.checkIdentity
    try await Task.detached {
      let file = directory.appendingPathComponent(name)
      try Self.coordinateWriting(file) { url in
        try checkIdentity()
        try Self.rejectSymlinks([directory.deletingLastPathComponent(), directory, url])
        try FileManager.default.startDownloadingUbiquitousItem(at: url)
      }
    }.value
  }

  public func copy(_ name: String, in folder: String, to destination: URL) async throws {
    try Self.validate(name)
    let resolved = try await folderURL(folder)
    let directory = resolved.url
    let checkIdentity = resolved.checkIdentity
    try await Task.detached {
      let source = directory.appendingPathComponent(name)
      try Self.rejectSymlinks([directory.deletingLastPathComponent().deletingLastPathComponent(), directory.deletingLastPathComponent(), directory, source])
      try Self.requireDownloaded(source)
      try Self.coordinateReading(source, writing: destination.deletingLastPathComponent()) { sourceURL, destinationDirectory in
        try checkIdentity()
        try Self.rejectSymlinks([directory.deletingLastPathComponent(), directory, sourceURL])
        try Self.requireDownloaded(sourceURL)
        try Self.requireRegularFile(sourceURL)
        let target = destinationDirectory.appendingPathComponent(destination.lastPathComponent)
        try Self.copyStaged(sourceURL, to: target, checkIdentity: checkIdentity)
      }
    }.value
  }

  private static func rejectSymlinks(_ urls: [URL]) throws {
    for url in urls {
      var attributes = stat()
      if lstat(url.path, &attributes) == 0 {
        if attributes.st_mode & S_IFMT == S_IFLNK { throw Error.symbolicLink }
      } else if errno != ENOENT {
        throw POSIXError(POSIXErrorCode(rawValue: errno) ?? .EIO)
      }
    }
  }

  private static func requireRegularFile(_ url: URL) throws {
    guard try url.resourceValues(forKeys: [.isRegularFileKey]).isRegularFile == true else {
      throw Error.notRegularFile
    }
  }

  private static func requireDownloaded(_ url: URL) throws {
    let values = try url.resourceValues(forKeys: [.isUbiquitousItemKey, .ubiquitousItemDownloadingStatusKey])
    if values.isUbiquitousItem == true && values.ubiquitousItemDownloadingStatus != .current {
      throw Error.downloadRequired
    }
  }

  private static func copyStaged(_ source: URL, to destination: URL, checkIdentity: () throws -> Void) throws {
    let manager = FileManager.default
    guard !manager.fileExists(atPath: destination.path) else { throw Error.alreadyExists }
    let replacement = try manager.url(for: .itemReplacementDirectory, in: .userDomainMask, appropriateFor: destination, create: true)
    defer { try? manager.removeItem(at: replacement) }
    let staging = replacement.appendingPathComponent(UUID().uuidString)
    try manager.copyItem(at: source, to: staging)
    try checkIdentity()
    try manager.moveItem(at: staging, to: destination)
  }

  private static func coordinateWriting(_ url: URL, action: (URL) throws -> Void) throws {
    var coordinationError: NSError?
    var result: Result<Void, Swift.Error>?
    NSFileCoordinator().coordinate(writingItemAt: url, options: [], error: &coordinationError) { coordinated in
      result = Result { try action(coordinated) }
    }
    if let coordinationError { throw coordinationError }
    guard let result else { throw Error.coordinationFailed }
    try result.get()
  }

  private static func coordinateReading(_ source: URL, writing destination: URL, action: (URL, URL) throws -> Void) throws {
    var coordinationError: NSError?
    var result: Result<Void, Swift.Error>?
    NSFileCoordinator().coordinate(readingItemAt: source, options: [], writingItemAt: destination, options: [], error: &coordinationError) { readURL, writeURL in
      result = Result { try action(readURL, writeURL) }
    }
    if let coordinationError { throw coordinationError }
    guard let result else { throw Error.coordinationFailed }
    try result.get()
  }
}
