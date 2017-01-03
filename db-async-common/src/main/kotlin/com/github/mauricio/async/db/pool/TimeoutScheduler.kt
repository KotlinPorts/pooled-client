package com.github.mauricio.async.db.pool

import com.github.elizarov.async.suspendable
import com.github.elizarov.async.withTimeout
import java.util.concurrent.atomic.AtomicBoolean
import io.netty.channel.EventLoopGroup
import java.time.Duration
import java.util.concurrent.CancellationException
import java.util.concurrent.TimeUnit
import java.util.concurrent.TimeoutException
import kotlin.coroutines.Continuation
import kotlin.coroutines.ContinuationDispatcher
import kotlin.coroutines.suspendCoroutine

interface TimeoutScheduler {

    val isTimeoutedBool: AtomicBoolean

    /**
     *
     * The event loop group to be used for scheduling.
     *
     * @return
     */

    val isTimeouted : Boolean get() = isTimeoutedBool.get()

    fun eventLoopGroup(): EventLoopGroup

    fun executionContext(): ContinuationDispatcher

    suspend fun <T> addTimeout(durationMillis: Long?, block: (Continuation<T>) -> Unit): T = suspendable(executionContext()) {
        try {
            withTimeout<T>(eventLoopGroup(), durationMillis) {
                suspendCoroutine<T>(block)
            }
        } catch (e: CancellationException) {
            onTimeout()
            throw TimeoutException("Operation is timeouted after it took too long to return (${durationMillis})")
        }
    }

    suspend fun <T> addTimeout(duration: Duration?, block: (Continuation<T>) -> Unit) =
            addTimeout(duration?.toMillis(), block)

    fun onTimeout()
}
