package com.github.elizarov.async

import kotlin.coroutines.Continuation
import kotlin.coroutines.startCoroutine
import kotlin.coroutines.suspendCoroutine

/**
 * @author Ivan Popelyshev
 */

/**
 * used as trivial suspend, will be removed for Kotlin 1.1 RC
 */
suspend fun <T> suspendable(block: suspend CustomController<T>.() -> T): T = suspendCoroutine<T> {
    cont ->
    val controller = CustomController<T>(cont)
    block.startCoroutine(controller, controller)
}

class CustomController<T>(val current: Continuation<T>) : Continuation<T> {
    override fun resume(value: T) {
        current.resume(value)
    }

    override fun resumeWithException(exception: Throwable) {
        current.resumeWithException(exception)
    }
}
