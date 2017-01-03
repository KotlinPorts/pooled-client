package com.github.elizarov.async

import io.netty.channel.EventLoopGroup
import io.netty.channel.nio.NioEventLoopGroup
import java.time.Duration
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.TimeUnit
import kotlin.coroutines.Continuation
import kotlin.coroutines.ContinuationDispatcher
import kotlin.coroutines.startCoroutine


suspend fun <T> withTimeout(service: ScheduledExecutorService, duration: Duration?, block: suspend () -> T): T =
        withTimeout(service, duration?.toMillis(), block)

suspend fun <T> withTimeout(service: ScheduledExecutorService, millis: Long?, block: suspend () -> T): T =
        suspendCancellableDispatchedCoroutine { c: CancellableContinuation<T>, d: ContinuationDispatcher? ->
            // schedule cancellation of this continuation on time
            val timeout = if (millis != null) service.schedule({ c.cancel() }, millis, java.util.concurrent.TimeUnit.MILLISECONDS)
            else null;
            // restart block in a separate coroutine
            block.startCoroutine(completion = object : Continuation<T> {
                // on completion cancel timeout
                override fun resume(value: T) {
                    timeout?.cancel(false)
                    c.resume(value)
                }

                override fun resumeWithException(exception: Throwable) {
                    timeout?.cancel(false)
                    c.resumeWithException(exception)
                }
            }, dispatcher = CancellableDispatcher(c, d)) // use cancellable dispatcher inside
        }
