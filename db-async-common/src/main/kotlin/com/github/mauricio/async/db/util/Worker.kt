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

import mu.KLogging
import java.util.concurrent.ExecutorService
import kotlin.coroutines.Continuation
import kotlin.coroutines.ContinuationDispatcher
import kotlin.coroutines.startCoroutine
import kotlin.coroutines.suspendCoroutine

class Worker(val executorService: ExecutorService) {

    constructor() : this(ExecutorServiceUtils.newFixedPool(1, "db-async-worker"))

    companion object : KLogging()

    val dispatcher = object : ContinuationDispatcher {
        override fun <T> dispatchResume(value: T, continuation: Continuation<T>): Boolean {
            executorService.execute({
                continuation.resume(value)
            })
            return true;
        }

        override fun dispatchResumeWithException(exception: Throwable, continuation: Continuation<*>): Boolean {
            executorService.execute({
                continuation.resumeWithException(exception)
            })
            return true;
        }
    }

    //TODO: write dispatcher for this worker

    /**
     * popelyshev:
     *
     * For now we assume that everything will be done inside this executorService
     * The only async thing inside will be "checkout" method which actually acts as our dispatcher
     */
    suspend fun <A> act(f: suspend () -> A) = suspendCoroutine<A> {
        cont ->
        f.startCoroutine(cont, dispatcher)
    }

    fun action(f: () -> Unit) {
        executorService.execute({
            try {
                f()
            } catch (e: Throwable) {
                logger.error("Failed to execute task %s".format(f), e)
            }
        })
    }

    fun shutdown() {
        this.executorService.shutdown()
    }
}
