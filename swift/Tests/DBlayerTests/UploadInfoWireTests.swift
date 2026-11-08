import Testing
@testable import DBlayer

struct UploadInfoWireTests {
  @Test
  func uploadInfoCarriesLocalUrlAndProcessingOfTheWireUpload() {
    let wire = WireUpload(
      status: .uploading, stage: "put", sentBytes: 1, totalBytes: 4, progress: 25, waitReason: nil, attempts: 0, error: nil,
      localUrl: "file:///tmp/clip.mov", processing: WireProcessing(progress: 0.5)
    )
    let info = UploadInfo(wire: wire)
    #expect(info.localUrl == "file:///tmp/clip.mov")
    #expect(info.processing == UploadProcessing(progress: 0.5))
  }
}
