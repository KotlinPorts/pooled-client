package com.github.kotlinports.pooled.core

import java.util.concurrent.TimeUnit
import kotlin.coroutines.CoroutineContext

interface Deferred<out T> {
    /**
     * if it gives CancellationException, then it came from above,
     * subroutine will be executed anyway
     */
    suspend fun await(): T
}

/**
 * Allows to start
 */
interface ExecutionContext {
    /**
     * Kotlin Coroutine Context, contains Interceptor and marker for current thread
     */
    val ktContext: CoroutineContext

    /**
     * run suspend block
     */
    suspend fun run(block: suspend () -> Unit)

    /**
     * run single block in this context
     */
    fun runTask(block: () -> Unit)

    /**
     * run suspend block, use the ktContext to test the stuff
     * whitewash for Cancellable coroutine context key
     */
    fun <T> defer(block: suspend () -> T): Deferred<T>

    /**
     * sets timer on specific context
     * @param delay milliseconds for delay
     */
    fun setTimer(name: String, delay: Long, handler: suspend (timerId: Long) -> Unit): Long

    /**
     * clears timer
     */
    fun clearTimer(timerId: Long)
}

/**
 * Pre-configured service, it knows how many threads it gives for our purpose
 */
interface ExecutionService {
    /**
     * Start using the service, get or creates contexts, increase refcount
     */
    fun open(): List<ExecutionContext>

    /**
     * Close the service, may be free contexts, decreases refcount
     */
    fun close(usedContexts: List<ExecutionContext>)
}
