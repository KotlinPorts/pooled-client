package com.github.elizarov.async

import kotlin.coroutines.Continuation
import kotlin.coroutines.ContinuationDispatcher

class CancellableDispatcher(
        cancellable: Cancellable,
        dispatcher: ContinuationDispatcher? = null
) : Cancellable by cancellable, ContinuationDispatcher {

    private val delegate: ContinuationDispatcher? =
        if (dispatcher is CancellableDispatcher) dispatcher.delegate else dispatcher

    override fun <T> dispatchResume(value: T, continuation: Continuation<T>): Boolean {
        if (isCancelled) {
            val exception = CancellationException()
            if (dispatchResumeWithException(exception, continuation)) return true
            continuation.resumeWithException(exception) // todo: stack overflow?
            return true
        }
        return delegate != null && delegate.dispatchResume(value, continuation)
    }

    override fun dispatchResumeWithException(exception: Throwable, continuation: Continuation<*>): Boolean {
        return delegate != null && delegate.dispatchResumeWithException(exception, continuation)
    }
}
