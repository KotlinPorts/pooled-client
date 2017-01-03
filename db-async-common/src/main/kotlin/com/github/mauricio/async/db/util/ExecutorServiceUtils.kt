/*
 * Copyright 2013 Maurício Linhares
 *
 * Maurício Linhares licenses this file to you under the Apache License,
 * version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at:
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

package com.github.mauricio.async.db.util

import java.time.Duration
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import java.util.concurrent.ScheduledExecutorService
import kotlin.coroutines.Continuation
import kotlin.coroutines.ContinuationDispatcher

object ExecutorServiceUtils {
    val CachedThreadPool = Executors.newScheduledThreadPool(0, DaemonThreadsFactory("db-async-default"))

    //TODO: make something better, check if we already in this thread
    //Trivial implementation
    val CachedExecutionContext = object : ContinuationDispatcher {
        override fun <T> dispatchResume(value: T, continuation: Continuation<T>): Boolean {
            CachedThreadPool.execute {
                continuation.resume(value)
            }
            return true
        }

        override fun dispatchResumeWithException(exception: Throwable, continuation: Continuation<*>): Boolean {
            CachedThreadPool.execute {
                continuation.resumeWithException(exception)
            }
            return true
        }
    }

    fun newFixedPool(count: Int, name: String): ExecutorService =
            Executors.newFixedThreadPool(count, DaemonThreadsFactory(name))

    suspend fun <T> withTimeout(duration: Duration?, block: suspend () -> T): T =
        com.github.elizarov.async.withTimeout(CachedThreadPool, duration?.toMillis(), block)

    suspend fun <T> withTimeout(durationMillis: Long?, block: suspend () -> T): T =
        com.github.elizarov.async.withTimeout(CachedThreadPool, durationMillis ?: 0, block)
}
