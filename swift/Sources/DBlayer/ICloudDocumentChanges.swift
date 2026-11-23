import Foundation

public struct ICloudDocument: Sendable, Equatable {
  public enum DownloadStatus: Sendable, Equatable {
    case unknown, notDownloaded, downloading, downloaded, current, failed
  }

  public enum UploadStatus: Sendable, Equatable {
    case unknown, notUploaded, uploading, uploaded, failed
  }

  public let name: String
  public let downloadStatus: DownloadStatus
  public let uploadStatus: UploadStatus

  static func snapshot(items: [[String: Any]], folder: URL) -> [ICloudDocument] {
    items.compactMap { item in
      guard let url = item[NSMetadataItemURLKey] as? URL,
        url.deletingLastPathComponent().standardizedFileURL == folder.standardizedFileURL,
        !url.lastPathComponent.hasPrefix("."),
        (item["kMDItemFSInvisible"] as? Bool) != true,
        let types = item[NSMetadataItemContentTypeTreeKey] as? [String],
        types.contains("public.data"), !types.contains("public.symlink"), !types.contains("public.folder")
      else { return nil }

      let download: DownloadStatus
      if item[NSMetadataUbiquitousItemDownloadingErrorKey] != nil { download = .failed }
      else if (item[NSMetadataUbiquitousItemIsDownloadingKey] as? Bool) == true { download = .downloading }
      else {
        switch item[NSMetadataUbiquitousItemDownloadingStatusKey] as? String {
        case NSMetadataUbiquitousItemDownloadingStatusNotDownloaded: download = .notDownloaded
        case NSMetadataUbiquitousItemDownloadingStatusDownloaded: download = .downloaded
        case NSMetadataUbiquitousItemDownloadingStatusCurrent: download = .current
        default: download = .unknown
        }
      }
      let upload: UploadStatus
      if item[NSMetadataUbiquitousItemUploadingErrorKey] != nil { upload = .failed }
      else if (item[NSMetadataUbiquitousItemIsUploadingKey] as? Bool) == true { upload = .uploading }
      else if let uploaded = item[NSMetadataUbiquitousItemIsUploadedKey] as? Bool {
        upload = uploaded ? .uploaded : .notUploaded
      } else { upload = .unknown }
      return ICloudDocument(name: url.lastPathComponent, downloadStatus: download, uploadStatus: upload)
    }.sorted { $0.name < $1.name }
  }
}

extension ICloudDocuments {
  public func changes(in folder: String) -> AsyncThrowingStream<[ICloudDocument], Swift.Error> {
    AsyncThrowingStream(bufferingPolicy: .bufferingNewest(1)) { continuation in
      let task = Task { @MainActor in
        do {
          let resolved = try await folderURL(folder)
          try Task.checkCancellation()
          let observation = ICloudDocumentObservation(folder: resolved, continuation: continuation)
          await withTaskCancellationHandler {
            observation.start()
            await observation.wait()
          } onCancel: {
            Task { @MainActor in observation.stop() }
          }
        } catch {
          continuation.finish(throwing: error)
        }
      }
      continuation.onTermination = { _ in task.cancel() }
    }
  }
}

@MainActor
private final class ICloudDocumentObservation {
  private let folder: ICloudDocuments.ResolvedFolder
  private let continuation: AsyncThrowingStream<[ICloudDocument], Swift.Error>.Continuation
  private let query = NSMetadataQuery()
  private let keys = [
    NSMetadataItemURLKey, NSMetadataItemContentTypeTreeKey, "kMDItemFSInvisible",
    NSMetadataUbiquitousItemDownloadingStatusKey, NSMetadataUbiquitousItemIsDownloadingKey,
    NSMetadataUbiquitousItemDownloadingErrorKey, NSMetadataUbiquitousItemIsUploadedKey,
    NSMetadataUbiquitousItemIsUploadingKey, NSMetadataUbiquitousItemUploadingErrorKey,
  ]
  private var observers: [NSObjectProtocol] = []
  private var previous: [ICloudDocument]?
  private var stopped = false
  private var gathered = false
  private var waiter: CheckedContinuation<Void, Never>?

  init(folder: ICloudDocuments.ResolvedFolder, continuation: AsyncThrowingStream<[ICloudDocument], Swift.Error>.Continuation) {
    self.folder = folder
    self.continuation = continuation
  }

  func start() {
    query.searchScopes = [NSMetadataQueryUbiquitousDocumentsScope]
    query.predicate = NSPredicate(format: "%K BEGINSWITH %@", NSMetadataItemPathKey, folder.url.path + "/")
    let center = NotificationCenter.default
    observers.append(center.addObserver(forName: .NSMetadataQueryDidFinishGathering, object: query, queue: .main) { [self] _ in
      MainActor.assumeIsolated { gathered = true; publish() }
    })
    observers.append(center.addObserver(forName: .NSMetadataQueryDidUpdate, object: query, queue: .main) { [self] _ in
      MainActor.assumeIsolated { if gathered { publish() } }
    })
    observers.append(center.addObserver(forName: .NSUbiquityIdentityDidChange, object: nil, queue: .main) { [self] _ in
      MainActor.assumeIsolated {
        do { try folder.checkIdentity() } catch { finish(error) }
      }
    })
    guard query.start() else { finish(ICloudDocuments.Error.unavailable); return }
  }

  func wait() async {
    await withCheckedContinuation { continuation in
      if stopped { continuation.resume() } else { waiter = continuation }
    }
  }

  private func publish() {
    guard !stopped else { return }
    do { try folder.checkIdentity() } catch { finish(error); return }
    query.disableUpdates()
    defer { query.enableUpdates() }
    let items = (0..<query.resultCount).compactMap { index -> [String: Any]? in
      guard let item = query.result(at: index) as? NSMetadataItem else { return nil }
      var values: [String: Any] = [:]
      for key in keys { values[key] = item.value(forAttribute: key) }
      return values
    }
    let snapshot = ICloudDocument.snapshot(items: items, folder: folder.url)
    guard snapshot != previous else { return }
    previous = snapshot
    continuation.yield(snapshot)
  }

  private func finish(_ error: Swift.Error) {
    stop()
    continuation.finish(throwing: error)
  }

  func stop() {
    guard !stopped else { return }
    stopped = true
    query.stop()
    observers.forEach { NotificationCenter.default.removeObserver($0) }
    observers.removeAll()
    waiter?.resume()
    waiter = nil
  }
}
