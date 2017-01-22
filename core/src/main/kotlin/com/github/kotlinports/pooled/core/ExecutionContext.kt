package com.github.kotlinports.pooled.core

import kotlin.coroutines.CoroutineContext

/**
 * Allows to start
 */
interface ExecutionContext {
    /**
     * Kotlin Coroutine Context, contains Interceptor and marker for current thread
     */
    val ktContext: CoroutineContext

    /**
     * run single block in this context
     */
    fun runTask(block: () -> Unit)

    /**
     * run suspend block, use the ktContext to test the stuff
     */
    suspend fun <T> runTask(block: suspend () -> Unit): T

    /**
     * run single suspend block in this context, the block is already safe
     */
    fun launch(block: suspend () -> Unit)

    /**
     * sets timer on specific context
     */
    fun setTimer(name: String, delay: Long, periodic: Boolean, handler: suspend (timerId: Long) -> Unit): Long

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
