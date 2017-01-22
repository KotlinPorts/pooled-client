package com.github.kotlinports.pooled.core

/**
 * Raised when a pool has reached it's limit of available objects.
 *
 * @param message
 */
class PoolExhaustedException( message : String ) :  IllegalStateException( message )

/**
 * Thrown when the pool has already been closed.
 */
class PoolAlreadyTerminatedException :  IllegalStateException( "This pool has already been terminated" )

interface AsyncObjectPool<T> {

    /**
     * Returns an object from the pool. If the pool can not create or enqueue
     * requests it will raise an
     * [[PoolExhaustedException]].
     *
     * @return object from the pool
     */

    suspend fun take(): T

    /**
     * Returns an object taken from the pool back to it. This object will become available for another client to use.
     * If the object is invalid or can not be reused for some reason
     * it will raise the error that prevented this object of being added back to the pool.
     * The object is then discarded from the pool.
     *
     * @param item
     * @return
     */

    suspend fun giveBack(item: T)

    /**
     * Closes this pool and future calls to **take** will cause the [[PoolAlreadyTerminatedException]].
     *
     * @return
     */

    suspend fun close()

    /**
     * Retrieve and use an object from the pool for a single computation, returning it when the operation completes.
     *
     * @param block
     * @return whatever inner block returns
     */

    //sorry, no use here
    suspend fun <A> use(block: suspend (t: T) -> A) : A {
        val item = take()

        try {
            return block(item)
        } finally {
            giveBack(item)
        }
    }

}
