package com.github.elizarov.async

import java.util.concurrent.atomic.AtomicReferenceFieldUpdater
import kotlin.coroutines.*
import kotlin.coroutines.CoroutineIntrinsics.SUSPENDED

// --------------- cancellable continuations ---------------

public interface CancellableContinuation<in T> : Continuation<T>, Cancellable {
    public fun cancel()
}

public inline suspend fun <T> suspendCancellableCoroutine(crossinline block: (CancellableContinuation<T>) -> Unit): T =
    suspendDispatchedCoroutineOrReturn { c, d ->
        val safe = SafeCancellableContinuation(c, d as? Cancellable)
        block(safe)
        safe.getResult()
    }

public inline suspend fun <T> suspendCancellableDispatchedCoroutine(crossinline block: (CancellableContinuation<T>, ContinuationDispatcher?) -> Unit): T =
    suspendDispatchedCoroutineOrReturn { c, d ->
        val safe = SafeCancellableContinuation(c, d as? Cancellable)
        block(safe, d)
        safe.getResult()
    }


public fun <T> (suspend () -> T).createCancellableCoroutine(
        completion: Continuation<T>,
        dispatcher: ContinuationDispatcher? = null
): CancellableContinuation<Unit> {
    return SafeCancellableContinuation(
        createCoroutine(completion = completion, dispatcher = dispatcher),
        dispatcher as? Cancellable)
}

// --------------- implementation details ---------------

@PublishedApi
internal class SafeCancellableContinuation<in T>(
        private val delegate: Continuation<T>,
        cancellable: Cancellable?
) : CancellationScope(), CancellableContinuation<T>, CancelHandler {
    private val registration = cancellable?.registerCancelHandler(this)

    private class Fail(val exception: Throwable)
    private class Suspended(val state: Any?)

    private companion object {
        @JvmStatic
        val SUSPENDED_ACTIVE = Suspended(ACTIVE) // Optimization (when no cancellation required)

        @JvmStatic
        val RESUMED: Any? = Any()

        @JvmStatic
        fun makeSuspended(state: Any?) = if (state == ACTIVE) SUSPENDED_ACTIVE else Suspended(state)
    }

    override fun unwrapState(state: Any?): Any? = (state as? Suspended)?.state ?: state

    override fun rewrapState(prevState: Any?, newState: Any?): Any? =
        if (prevState is Suspended) makeSuspended(newState) else newState

    override fun resume(value: T) {
        while (true) { // lock-free loop on state
            val cur = state // atomic read
            when (cur) {
                is ActiveNode -> if (compareAndSetState(cur, value)) {
                    complete()
                    return
                }
                is Suspended -> if (compareAndSetState(cur, RESUMED)) {
                    complete()
                    delegate.resume(value)
                    return
                }
                CANCELLED -> return // ignore resumes on cancelled continuation
                else -> throw IllegalStateException("Already resumed")
            }
        }
    }

    override fun resumeWithException(exception: Throwable) {
        while (true) { // lock-free loop on state
            val cur = state // atomic read
            when (cur) {
                is ActiveNode -> if (compareAndSetState(cur, Fail(exception))) {
                    complete()
                    return
                }
                is Suspended -> if (compareAndSetState(cur, RESUMED)) {
                    complete()
                    delegate.resumeWithException(exception)
                    return
                }
                CANCELLED -> return // ignore resumes on cancelled continuation
                else -> throw IllegalStateException("Already resumed")
            }
        }
    }

    private fun complete() {
        registration?.unregisterCancelHandler()
    }

    fun getResult(): Any? {
        while (true) { // lock-free loop on state
            val cur = state // atomic read
            when (cur) {
                is ActiveNode -> if (compareAndSetState(cur, makeSuspended(cur))) return SUSPENDED
                CANCELLED -> throw CancellationException()
                RESUMED -> return SUSPENDED // already called continuation, indicate SUSPENDED upstream
                is Fail -> throw cur.exception
                else -> return cur // result value must be there
            }
        }
    }

    // cancellation support

    override fun handleCancel(cancellable: Cancellable) {
        cancel()
    }

    override fun afterCancel(prevState: Any?, suppressedException: Throwable?) {
        complete()
        if (prevState is Suspended) {
            val cancellationException = CancellationException()
            if (suppressedException != null) cancellationException.addSuppressed(suppressedException)
            delegate.resumeWithException(cancellationException)
        }
        if (suppressedException != null) throw suppressedException
    }
}


