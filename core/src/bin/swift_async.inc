fileprivate func uniffiRustCallAsync<F, T>(
    rustFutureFunc: () -> UInt64,
    cancelFunc: ((UInt64) -> ())? = nil,
    pollFunc: (UInt64, @escaping UniffiRustFutureContinuationCallback, UInt64) -> (),
    completeFunc: (UInt64, UnsafeMutablePointer<RustCallStatus>) -> F,
    freeFunc: @escaping (UInt64) -> (),
    liftFunc: (F) throws -> T,
    errorHandler: ((RustBuffer) throws -> Swift.Error)?
) async throws -> T {
    uniffiEnsureDblayerInitialized()
    let handle = RustFutureHandle(rustFutureFunc(), cancel: cancelFunc, free: freeFunc)
    defer { handle.free() }
    return try await withTaskCancellationHandler {
        var pollResult: Int8
        repeat {
            pollResult = await withUnsafeContinuation {
                pollFunc(
                    handle.value,
                    { handle, pollResult in
                        RuntimeLane.run { uniffiFutureContinuationCallback(handle: handle, pollResult: pollResult) }
                    },
                    uniffiContinuationHandleMap.insert(obj: $0)
                )
            }
        } while pollResult != UNIFFI_RUST_FUTURE_POLL_READY
        return try liftFunc(makeRustCall(
            { completeFunc(handle.value, $0) },
            errorHandler: errorHandler
        ))
    } onCancel: {
        handle.cancel()
    }
}

