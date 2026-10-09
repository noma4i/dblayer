import Testing
@testable import DBlayer

struct LiveSourceTests {
  struct Refused: Error {}

  @Test
  func aSubscribeThatThrowsEndsTheStreamAndCurrentReturnsTheInitialValue() async {
    let source = LiveSource<Int>(initial: 7) { _ in throw Refused() }
    let value = await withTaskGroup(of: Int?.self) { group in
      group.addTask { await source.current() }
      group.addTask {
        try? await Task.sleep(for: .seconds(3))
        return nil
      }
      let first = await group.next() ?? nil
      group.cancelAll()
      return first
    }
    #expect(value == 7)
  }
}
