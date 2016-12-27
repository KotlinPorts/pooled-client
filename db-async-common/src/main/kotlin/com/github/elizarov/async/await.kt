package com.github.elizarov.async

import java.util.concurrent.CompletableFuture

suspend fun <T> CompletableFuture<T>.await(): T =
    suspendCancellableCoroutine { c ->
        val g = whenComplete { result, exception ->
            if (exception == null) // the future has been completed normally
                c.resume(result)
            else // the future has completed with an exception
                c.resumeWithException(exception)
        }
        c.registerCancelHandler(object : CancelHandler {
            override fun handleCancel(cancellable: Cancellable) {
                g.cancel(false)
            }
        })
    }
