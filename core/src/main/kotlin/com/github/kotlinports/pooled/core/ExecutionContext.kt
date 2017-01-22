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

    /**
     * sets timer on specific context
     */
    fun setTimer(context: ExecutionContext, delay: Long, periodic: Boolean, handler: suspend (timerId: Long) -> Unit): Long

    /**
     * clears timer
     */
    fun clearTimer(timerId: Long)
}
