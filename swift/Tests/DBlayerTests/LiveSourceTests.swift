import Testing
@testable import DBlayer

struct LiveSourceTests {
  struct Refused: Error {}

  @Test
  func aSubscribeThatThrowsSurfacesItsErrorFromStreamAndCurrent() async {
    let source = LiveSource<Int>(initial: 7) { _ in throw Refused() }
    await #expect(throws: Refused.self) { try await source.current() }
    await #expect(throws: Refused.self) { _ = try await source.stream() }
  }
}
