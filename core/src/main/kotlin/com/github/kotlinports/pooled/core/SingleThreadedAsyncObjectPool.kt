package com.github.kotlinports.pooled.core

import kotlinx.coroutines.experimental.launch
import mu.KLogging
import java.util.*
import java.util.concurrent.atomic.AtomicLong
import kotlin.coroutines.Continuation
import kotlin.coroutines.suspendCoroutine

open class SingleThreadedAsyncObjectPool<T>(
        val factory: ObjectFactory<T>,
        val configuration: PoolConfiguration,
        val executionContext: ExecutionContext
) : AsyncObjectPool<T> {
    companion object : KLogging() {
        val Counter = AtomicLong()
    }

    private var poolables = listOf<PoolableHolder<T>>()
    private val checkouts = ArrayList<T>(configuration.maxObjects)
    private val waitQueue: Queue<Continuation<T>> = LinkedList<Continuation<T>>()
    private val timer = Timer("async-object-pool-timer-" + Counter.incrementAndGet(), true)

    init {
        timer.scheduleAtFixedRate(object : TimerTask() {
            override fun run() {
                launch(mainPool.cc) { testObjects() }
            }
        }, configuration.validationInterval, configuration.validationInterval)
    }

    private var closed = false

    /**
     *
     * Asks for an object from the pool, this object should be returned to the pool when not in use anymore.
     *
     * @return
     */

    override suspend fun take(): T = suspendCoroutine {
        cont ->
        if (this.closed) {
            cont.resumeWithException(PoolAlreadyTerminatedException())
        }
        checkout(cont)
    }

    /**
     *
     * Returns an object to the pool. The object is validated before being added to the collection
     * of available objects to make sure we have a usable object. If the object isn't valid it's discarded.
     *
     * @param item
     * @return
     */

    override suspend fun giveBack(item: T) = mainPool.act<Unit>
    {
        // Ensure it came from this pool
        val idx = this.checkouts.indexOf(item)
        if (idx >= 0) {
            this.checkouts.removeAt(idx)
            try {
                factory.validate(item)
                addBack(item)
            } catch(e: Exception) {
                factory.destroy(item)
                throw e
            }
        } else {
            // It's already a failure but lets doublecheck why
            val isFromOurPool = this.poolables.find({ item == it.item }) != null

            if (isFromOurPool) {
                throw IllegalStateException("This item has already been returned")
            } else {
                throw IllegalArgumentException("The returned item did not come from this pool.")
            }
        }
    }

    fun isFull(): Boolean = poolables.isEmpty() && checkouts.size == configuration.maxObjects

    override suspend fun close() = run {
        try {
            mainPool.act<Unit> {
                if (!closed) {
                    timer.cancel()
                    mainPool.shutdown()
                    closed = true
                    (poolables.map({ it.item }) + checkouts)
                            .forEach({ factory.destroy(it) })
                }
            }
        } catch (e: RejectedExecutionException) {
            return@run this@SingleThreadedAsyncObjectPool
        }
        this@SingleThreadedAsyncObjectPool
    }

    fun availables(): Iterable<T> = this.poolables.map { it.item }

    fun inUse(): Iterable<T> = this.checkouts

    fun queued(): Iterable<Continuation<T>> = this.waitQueue

    fun isClosed(): Boolean = this.closed

    /**
     *
     * Adds back an object that was in use to the list of poolable objects.
     *
     * @param item
     * @param promise
     */

    private suspend fun addBack(item: T) = run<AsyncObjectPool<T>>
    {
        poolables += PoolableHolder<T>(item)

        if (waitQueue.isNotEmpty()) {
            checkout(waitQueue.poll())
        }

        this@SingleThreadedAsyncObjectPool
    }

    /**
     *
     * Enqueues a promise to be fulfilled in the future when objects are sent back to the pool. If
     * we have already reached the limit of enqueued objects, fail the promise.
     *
     * @param promise
     */

    private fun enqueuePromise(cont: Continuation<T>) {
        if (this.waitQueue.size >= configuration.maxQueueSize) {
            val exception = PoolExhaustedException("There are no objects available and the waitQueue is full")
            exception.fillInStackTrace()
            cont.resumeWithException(exception)
        } else {
            this.waitQueue += cont
        }
    }

    /**
     * popelyshev:
     *
     * We assume that all methods that are calle from the worker
     * will call this method, that's why it acts like a dispatcher
     */
    private fun checkout(cont: Continuation<T>) {
        mainPool.action {
            if (this.isFull()) {
                this.enqueuePromise(cont)
            } else {
                this.createOrReturnItem(cont)
            }
        }
    }

    /**
     *
     * Checks if there is a poolable object available and returns it to the promise.
     * If there are no objects available, create a new one using the factory and return it.
     *
     * @param promise
     */

    private fun createOrReturnItem(cont: Continuation<T>) {
        if (poolables.isEmpty()) {
            mainPool.action {
                try {
                    val item = factory.create()
                    this.checkouts += item
                    cont.resume(item)
                } catch (e: Throwable) {
                    cont.resumeWithException(e)
                }
            }
        } else {
            val h = poolables[0]
            poolables = poolables.drop(1)
            val item = h.item
            this.checkouts += item
            cont.resume(item)
        }
    }

    protected fun finalize() {
        launch(mainPool.cc) { close() }
    }

    /**
     *
     * Validates pooled objects not in use to make sure they are all usable, great if
     * you're holding onto network connections since you can "ping" the destination
     * to keep the connection alive.
     *
     */

    private suspend fun testObjects() = mainPool.act {
        val removals = mutableListOf<PoolableHolder<T>>()
        this.poolables.forEach {
            poolable ->
            try {
                factory.test(poolable.item)
                if (poolable.timeElapsed() > configuration.maxIdle) {
                    logger.debug("Connection was idle for {}, maxIdle is {}, removing it", poolable.timeElapsed(), configuration.maxIdle)
                    removals += poolable
                    factory.destroy(poolable.item)
                }
            } catch (e: Throwable) {
                logger.error("Failed to validate object", e)
                removals += poolable
                factory.destroy(poolable.item)
            }
        }
        this.poolables = this.poolables - removals
    }

    private class PoolableHolder<T>(val item: T) {
        val time = System.currentTimeMillis()

        fun timeElapsed() = System.currentTimeMillis() - time
    }

}
