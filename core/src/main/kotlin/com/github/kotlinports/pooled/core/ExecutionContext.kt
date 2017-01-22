package com.github.kotlinports.pooled.core

import kotlin.coroutines.CoroutineContext

interface ExecutionContext {
    /**
     * Kotlin Coroutine Context, contains Interceptor and marker for current thread
     */
    val ktContext: CoroutineContext
}

/**
 * If you pass this, it means you already allocated it somehow, set it ref count and such
 */
interface ExecutionService {
    /**
     * Start using the service, get or creates contexts, increase refcount for multi-purpose service
     */
    fun open(): List<ExecutionContext>

    /**
     * Close the service, may be free contexts, decreases refcount for multi-purpose service
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
