package com.github.mauricio.async.db.pool

import com.github.elizarov.async.suspendable
import com.github.mauricio.async.db.util.ExecutorServiceUtils
import com.github.mauricio.async.db.QueryResult
import com.github.mauricio.async.db.Connection
import kotlin.coroutines.ContinuationDispatcher
import kotlin.coroutines.suspendCoroutine

class PartitionedConnectionPool<T : Connection>(
        factory: ObjectFactory<T>,
        configuration: PoolConfiguration,
        numberOfPartitions: Int)
    : PartitionedAsyncObjectPool<T>(factory, configuration, numberOfPartitions), Connection {

    override suspend fun disconnect(): Connection = suspendable {
        if (isConnected) {
            close()
        }
        this@PartitionedConnectionPool
    }

    override suspend fun connect(): Connection = suspendCoroutine {
        cont ->
        cont.resume(this)
    }

    override val isConnected: Boolean get() = !this.isClosed()

    override suspend fun sendQuery(query: String): QueryResult =
        this.use { sendQuery(query) }

    override suspend fun sendPreparedStatement(query: String, values: List<Any?>): QueryResult =
        this.use { sendPreparedStatement(query, values) }

    override suspend fun <A> inTransaction(dispatcher: ContinuationDispatcher?, f: suspend (conn: Connection) -> A) =
        this.use(dispatcher) { it.inTransaction(dispatcher, f) }
}
