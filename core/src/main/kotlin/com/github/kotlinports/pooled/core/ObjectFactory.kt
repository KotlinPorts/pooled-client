/**
 * This file contents derive from from https://github.com/mauricio/postgresql-async
 */

package com.github.kotlinports.pooled.core

interface ObjectFactory<T> {
    /**
     *
     * Creates a valid object to be used in the pool. This method can block if necessary to make sure a correctly built
     * is created.
     *
     * @return
     */

    suspend fun create() : T

    /**
     *
     * This method should "close" and release all resources acquired by the pooled object. This object will not be used
     * anymore so any cleanup necessary to remove it from memory should be made in this method. Implementors should not
     * raise an exception under any circumstances, the factory should async.log and clean up the exception itself.
     *
     * @param item
     */

    suspend fun destroy( item : T )

    /**
     *
     * Validates that an object can still be used for it's purpose. This method should test the object to make sure
     * it's still valid for clients to use. If you have a database connection, test if you are still connected, if you're
     * accessing a file system, make sure you can still see and change the file.
     *
     * You decide how fast this method should return and what it will test, you should usually do something that's fast
     * enough not to slow down the pool usage, since this call will be made whenever an object returns to the pool.
     *
     * @param item an object produced by this pool
     * @return
     */
    suspend fun validate( item : T )

    /**
     *
     * Does a full test on the given object making sure it's still valid. Different than validate, that's called whenever
     * an object is given back to the pool and should usually be fast, this method will be called when objects are
     * idle to make sure they don't "timeout" or become stale in anyway.
     *
     * For convenience, this method defaults to call **validate** but you can implement it in a different way if you
     * would like to.
     *
     * @param item an object produced by this pool
     * @return
     */

    suspend fun test( item : T )


}
