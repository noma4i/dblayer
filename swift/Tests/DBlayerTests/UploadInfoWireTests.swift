import Testing
@testable import DBlayer

struct UploadInfoWireTests {
  @Test
  func uploadInfoCarriesLocalUrlAndBatchProgressOfTheWireUpload() {
    let wire = WireUpload(
      status: .uploading, stage: "put", sentBytes: 1, totalBytes: 4, progress: 0.25, batchProgress: 0.75, waitReason: nil, attempts: 0, error: nil,
      localUrl: "file:///tmp/clip.mov"
    )
    let info = UploadInfo(wire: wire)
    #expect(info.localUrl == "file:///tmp/clip.mov")
    #expect(info.batchProgress == 0.75)
  }
}
