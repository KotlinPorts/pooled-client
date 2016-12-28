package com.github.mauricio.async.db.pool

import com.github.elizarov.async.suspendable
import java.util.concurrent.ConcurrentHashMap
import kotlin.coroutines.Continuation

open class PartitionedAsyncObjectPool<T>(
    val factory: ObjectFactory<T>,
    val configuration: PoolConfiguration,
    val numberOfPartitions: Int)
    :  AsyncObjectPool<T> {

    //TODO: why is it a map? why not array?
    private val pools = Array<Pair<Int, SingleThreadedAsyncObjectPool<T>>>(numberOfPartitions,
    {
        i ->
        Pair(i, SingleThreadedAsyncObjectPool<T>(factory, partitionConfig()))
    }).toMap()

    private val checkouts = ConcurrentHashMap<T, SingleThreadedAsyncObjectPool<T>>()

    override suspend fun take() = suspendable<T> {
        val pool = currentPool()
        val item = pool.take()
        checkouts.put(item, pool)
        item
    }

    override suspend fun giveBack(item: T) =
        checkouts
            .remove(item)!!
            .giveBack(item)

    override suspend fun close() = suspendable<PartitionedAsyncObjectPool<T>> {
        pools.values.forEach { it.close() }
        this@PartitionedAsyncObjectPool
    }

    fun availables(): Iterable<T> = pools.values.flatMap { it.availables() }

    fun inUse(): Iterable<T> = pools.values.flatMap { it.inUse() }

    fun queued(): Iterable<Continuation<T>> = pools.values.flatMap { it.queued() }

    protected fun isClosed() =
        pools.values.find { !it.isClosed() } == null

    private fun currentPool() =
        pools[currentThreadAffinity()]!!

    private fun currentThreadAffinity() =
        (Thread.currentThread().getId() % numberOfPartitions).toInt()

    private fun partitionConfig() =
        configuration.copy(
            maxObjects = configuration.maxObjects / numberOfPartitions,
            maxQueueSize = configuration.maxQueueSize / numberOfPartitions
        )
}