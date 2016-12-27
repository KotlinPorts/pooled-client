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

package com.github.mauricio.async.db.pool

import com.github.mauricio.async.db.util.Worker
import mu.KLogging
import org.funktionale.either.Either
import java.util.*
import java.util.concurrent.atomic.AtomicLong
import kotlin.coroutines.Continuation
import kotlin.coroutines.suspendCoroutine

/**
 *
 * Implements an [[com.github.mauricio.async.db.pool.AsyncObjectPool]] using a single thread from a
 * fixed executor service as an event loop to cause all calls to be sequential.
 *
 * Once you are done with this object remember to call it's close method to clean up the thread pool and
 * it's objects as this might prevent your application from ending.
 *
 * @param factory
 * @param configuration
 * @tparam T type of the object this pool holds
 */

open class SingleThreadedAsyncObjectPool<T>(
        val factory: ObjectFactory<T>,
        val configuration: PoolConfiguration
) : AsyncObjectPool<T> {
    companion object : KLogging() {
        val Counter = AtomicLong()
    }

    private val mainPool = Worker()
    private var poolables = listOf<PoolableHolder<T>>()
    private val checkouts = ArrayList<T>(configuration.maxObjects)
    private val waitQueue: Queue<Continuation<T>> = LinkedList<Continuation<T>>()
    private val timer = Timer("async-object-pool-timer-" + Counter.incrementAndGet(), true)

    init {
        timer.scheduleAtFixedRate(object : TimerTask() {
            override fun run() {
                mainPool.action {
                    testObjects()
                }
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

    override suspend fun take(): T {

        if (this.closed) {
            throw PoolAlreadyTerminatedException()
        }

        return checkout()
    }

    /**
     *
     * Returns an object to the pool. The object is validated before being added to the collection
     * of available objects to make sure we have a usable object. If the object isn't valid it's discarded.
     *
     * @param item
     * @return
     */

    override suspend fun giveBack(item: T): AsyncObjectPool[T]
    {
        val promise = Promise[AsyncObjectPool[T]]()
        this.mainPool.action {
            // Ensure it came from this pool
            val idx = this.checkouts.indexOf(item)
            if (idx >= 0) {
                this.checkouts.remove(idx)
                this.factory.validate(item) match {
                    case Success (item) => {
                    this.addBack(item, promise)
                }
                    case Failure (e) => {
                    this.factory.destroy(item)
                    promise.failure(e)
                }
                }
            } else {
                // It's already a failure but lets doublecheck why
                val isFromOurPool = (item match {
                    case x : AnyRef => this.poolables.find(holder => x eq holder . item . asInstanceOf [ AnyRef])
                    case _ => this.poolables.find(holder => item == holder . item)
                }).isDefined

                if (isFromOurPool) {
                    promise.failure(new IllegalStateException ("This item has already been returned"))
                } else {
                    promise.failure(new IllegalArgumentException ("The returned item did not come from this pool."))
                }
            }
        }

        promise.future
    }

    fun isFull: Boolean = this.poolables.isEmpty && this.checkouts.size == configuration.maxObjects

    fun close: Future[AsyncObjectPool[T]] =
    {
        try {
            val promise = Promise[AsyncObjectPool[T]]()
            this.mainPool.action {
                if (!this.closed) {
                    try {
                        this.timer.cancel()
                        this.mainPool.shutdown
                        this.closed = true
                        (this.poolables.map(i => i . item)++ this.checkouts).foreach(item => factory.destroy(item))
                        promise.success(this)
                    } catch {
                        case e : Exception => promise . failure (e)
                    }
                } else {
                    promise.success(this)
                }
            }
            promise.future
        } catch {
            case e : RejectedExecutionException if this.closed =>
            Future.successful(this)
        }
    }

    fun availables: Traversable[T] = this.poolables.map(item => item.item)

    fun inUse: Traversable[T] = this.checkouts

    fun queued: Traversable[Promise[T]] = this.waitQueue

    fun isClosed: Boolean = this.closed

    /**
     *
     * Adds back an object that was in use to the list of poolable objects.
     *
     * @param item
     * @param promise
     */

    private suspend fun addBack(item: T): AsyncObjectPool[T]
    {
        this.poolables :: = new PoolableHolder [ T](item)

        if (this.waitQueue.nonEmpty) {
            this.checkout(this.waitQueue.dequeue())
        }

        this
    }

    /**
     *
     * Enqueues a promise to be fulfilled in the future when objects are sent back to the pool. If
     * we have already reached the limit of enqueued objects, fail the promise.
     *
     * @param promise
     */

    private suspend fun enqueuePromise(): T {
        if (this.waitQueue.size >= configuration.maxQueueSize) {
            val exception = new PoolExhaustedException ("There are no objects available and the waitQueue is full")
            exception.fillInStackTrace()
            promise.failure(exception)
        } else {
            this.waitQueue += promise
        }
    }

    private suspend fun checkout(): T {
        this.mainPool.action {
            if (this.isFull()) {
                this.enqueuePromise(promise)
            } else {
                this.createOrReturnItem(promise)
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

    private fun createOrReturnItem(promise: Promise[T])
    {
        if (this.poolables.isEmpty) {
            try {
                val item = this.factory.create
                this.checkouts += item
                promise.success(item)
            } catch {
                case e : Exception => promise . failure (e)
            }
        } else {
            val h :: t = this.poolables
            this.poolables = t
            val item = h.item
            this.checkouts += item
            promise.success(item)
        }
    }

    override fun finalize() {
        this.close
    }

    /**
     *
     * Validates pooled objects not in use to make sure they are all usable, great if
     * you're holding onto network connections since you can "ping" the destination
     * to keep the connection alive.
     *
     */

    private fun testObjects() {
        val removals = mutableListOf<PoolableHolder<T>>()
        this.poolables.forEach {
            poolable ->
            val test = this.factory.test(poolable.item)
            when (test) {
                is Either.Left ->
                    if (poolable.timeElapsed() > configuration.maxIdle) {
                        logger.debug("Connection was idle for {}, maxIdle is {}, removing it", poolable.timeElapsed, configuration.maxIdle)
                        removals += poolable
                        factory.destroy(poolable.item)
                    }
                is Either.Right -> {
                    logger.error("Failed to validate object", test.right())
                    removals += poolable
                    factory.destroy(poolable.item)
                }
            }
        }
        this.poolables = this.poolables - removals
    }

    private class PoolableHolder<T>(val item: T) {
        val time = System.currentTimeMillis()

        fun timeElapsed() = System.currentTimeMillis() - time
    }
}

}
