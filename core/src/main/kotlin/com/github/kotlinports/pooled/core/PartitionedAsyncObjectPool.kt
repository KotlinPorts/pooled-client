package com.github.kotlinports.pooled.core

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import kotlin.coroutines.Continuation

open class PartitionedAsyncObjectPool<T>(
        val factory: ObjectFactory<T>,
        val configuration: PoolConfiguration,
        val numberOfPartitions: Int)
    : AsyncObjectPool<T> {

    //TODO: why is it a map? why not array?
    private val pools = Array<Pair<Int, SingleThreadedAsyncObjectPool<T>>>(numberOfPartitions,
    {
        i ->
        Pair(i, SingleThreadedAsyncObjectPool<T>(factory, partitionConfig()))
    }).toMap()

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
        pools.values.forEach { it.close() }
    }

    fun availables(): Iterable<T> = pools.values.flatMap { it.availables() }

    fun inUse(): Iterable<T> = pools.values.flatMap { it.inUse() }

    fun queued(): Iterable<Continuation<T>> = pools.values.flatMap { it.queued() }

    protected fun isClosed() =
        pools.values.find { !it.isClosed() } == null

    private fun currentPool() =
        pools[currentThreadAffinity()]!!

    private fun currentThreadAffinity() =
        (Thread.currentThread().id % numberOfPartitions).toInt()

    private fun partitionConfig() =
        configuration.copy(
            maxObjects = configuration.maxObjects / numberOfPartitions,
            maxQueueSize = configuration.maxQueueSize / numberOfPartitions
        )
}