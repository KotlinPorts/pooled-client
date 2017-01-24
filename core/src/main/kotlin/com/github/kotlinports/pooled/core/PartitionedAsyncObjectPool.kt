package com.github.kotlinports.pooled.core

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import kotlin.coroutines.Continuation

open class PartitionedAsyncObjectPool<T>(
        val factory: ObjectFactory<T>,
        val configuration: PoolConfiguration,
        val executionService: ExecutionService)
    : AsyncObjectPool<T> {

    private val threadList = executionService.open()

    private var closed = false

    private val numberOfPartitions: Int = threadList.size

    private val pools = threadList.map {
        SingleThreadedAsyncObjectPool<T>(factory, partitionConfig(), it)
    }

    private val checkouts = ConcurrentHashMap<T, SingleThreadedAsyncObjectPool<T>>()

    override suspend fun take(): T {
        Executors.newCachedThreadPool()
        val pool = currentPool()
        val item = pool.take()
        checkouts.put(item, pool)
        return item
    }

    override suspend fun giveBack(item: T) {
        checkouts
                .remove(item)!!
                .giveBack(item)
    }

    override suspend fun close() {
        pools.forEach { it.close() }
        executionService.close(threadList)
    }

    fun availables(): Iterable<T> = pools.flatMap { it.availables() }

    fun inUse(): Iterable<T> = pools.flatMap { it.inUse() }

    fun queued(): Iterable<Continuation<T>> = pools.flatMap { it.queued() }

    protected fun isClosed() = closed

    private fun currentPool() =
            pools[currentThreadAffinity()]

    private fun currentThreadAffinity() =
            (Thread.currentThread().id % numberOfPartitions).toInt()

    private fun partitionConfig() =
            configuration.copy(
                    maxObjects = configuration.maxObjects / numberOfPartitions,
                    maxQueueSize = configuration.maxQueueSize / numberOfPartitions
            )
}