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

import com.github.mauricio.async.db.util.ExecutorServiceUtils
import com.github.mauricio.async.db.QueryResult
import com.github.mauricio.async.db.Connection
import com.github.elizarov.async.suspendable
import kotlin.coroutines.ContinuationDispatcher
import kotlin.coroutines.suspendCoroutine

/**
 *
 * Pool specialized in database connections that also simplifies connection handling by
 * implementing the [[com.github.mauricio.async.db.Connection]] trait and saving clients from having to implement
 * the "give back" part of pool management. This lets you do your job without having to worry
 * about managing and giving back connection objects to the pool.
 *
 * The downside of this is that you should not start transactions or any kind of long running process
 * in this object as the object will be sent back to the pool right after executing a query. If you
 * need to start transactions you will have to take an object from the pool, do it and then give it
 * back manually.
 *
 * @param factory
 * @param configuration
 */

class ConnectionPool<T : Connection>(
        factory: ObjectFactory<T>,
        configuration: PoolConfiguration,
        val executionContext: ContinuationDispatcher? = null
)
    : SingleThreadedAsyncObjectPool<T>(factory, configuration), Connection {

    /**
     *
     * Closes the pool, you should discard the object.
     *
     * @return
     */

    override suspend fun disconnect() = suspendable<Connection> {
        if (isConnected) {
            close()
        }
        this@ConnectionPool
    }

    /**
     *
     * Always returns an empty map.
     *
     * @return
     */

    override suspend fun connect() = suspendCoroutine<Connection> { it.resume(this) }

    override val isConnected: Boolean get() = !this.isClosed()

    /**
     *
     * Picks one connection and runs this query against it. The query should be stateless, it should not
     * start transactions and should not leave anything to be cleaned up in the future. The behavior of this
     * object is undefined if you start a transaction from this method.
     *
     * @param query
     * @return
     */

    override suspend fun sendQuery(query: String): QueryResult =
            this.use(executionContext, { it.sendQuery(query) })

    /**
     *
     * Picks one connection and runs this query against it. The query should be stateless, it should not
     * start transactions and should not leave anything to be cleaned up in the future. The behavior of this
     * object is undefined if you start a transaction from this method.
     *
     * @param query
     * @param values
     * @return
     */

    override suspend fun sendPreparedStatement(query: String, values: List<Any?>): QueryResult =
            this.use(executionContext, { it.sendPreparedStatement(query, values) })

    /**
     *
     * Picks one connection and executes an (asynchronous) function on it within a transaction block.
     * If the function completes successfully, the transaction is committed, otherwise it is aborted.
     * Either way, the connection is returned to the pool on completion.
     *
     * @param f operation to execute on a connection
     * @return result of f, conditional on transaction operations succeeding
     */

    override suspend fun <A> inTransaction(dispatcher: ContinuationDispatcher?, f: suspend (conn: Connection) -> A): A =
            this.use(executionContext, { it.inTransaction(dispatcher ?: executionContext, f) })

}
