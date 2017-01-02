package com.github.elizarov.async

import kotlin.coroutines.Continuation
import kotlin.coroutines.ContinuationDispatcher
import kotlin.coroutines.startCoroutine
import kotlin.coroutines.suspendCoroutine

/**
 * @author Ivan Popelyshev
 */

/**
 * used as trivial suspend, will be removed for Kotlin 1.1 RC
 */
suspend fun <T> suspendable(dispatcher: ContinuationDispatcher? = null, block: suspend () -> T): T = suspendCoroutine<T> {
    cont ->
    block.startCoroutine(cont, dispatcher)
}
