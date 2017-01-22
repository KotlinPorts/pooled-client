package com.github.kotlinports.pooled.core

import kotlinx.coroutines.experimental.Job
import kotlinx.coroutines.experimental.launch
import kotlinx.coroutines.experimental.newFixedThreadPoolContext
import kotlinx.coroutines.experimental.run
import mu.KLogging
import kotlin.coroutines.*

class Worker(parent: Job? = null) {

    val cc = newFixedThreadPoolContext(1, "thrift-pool-worker", parent)


    companion object : KLogging()

    /**
     * popelyshev:
     *
     * For now we assume that everything will be done inside this executorService
     * The only async thing inside will be "checkout" method which actually acts as our dispatcher
     */
    suspend fun <A> act(f: suspend () -> A): A = run(cc, f)

    fun action(f: suspend () -> Unit) {
        launch(cc, {
            try {
                f()
            } catch (e: Throwable) {
                logger.error("Failed to execute task %s".format(f), e)
            }
        })
    }

    fun shutdown() {
        (cc.get(Job.Key) as Job).cancel()
    }
}
