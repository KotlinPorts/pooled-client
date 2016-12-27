package com.github.mauricio.async.db.pool

import com.github.mauricio.async.db.util.ExecutorServiceUtils
import com.github.mauricio.async.db.{ QueryResult, Connection }
import scala.concurrent.{ ExecutionContext, Future }

class PartitionedConnectionPool[T <: Connection](
    factory: ObjectFactory[T],
    configuration: PoolConfiguration,
    numberOfPartitions: Int,
    executionContext: ExecutionContext = ExecutorServiceUtils.CachedExecutionContext)
    :  PartitionedAsyncObjectPool[T](factory, configuration, numberOfPartitions)
    with Connection {

    fun disconnect: Future[Connection] = if (this.isConnected) {
        this.close.map(item => this)(executionContext)
    } else {
        Future.successful(this)
    }

    fun connect: Future[Connection] = Future.successful(this)

    fun isConnected: Boolean = !this.isClosed

    fun sendQuery(query: String): Future[QueryResult] =
        this.use(_.sendQuery(query))(executionContext)

    fun sendPreparedStatement(query: String, values: Seq[Any] = List()): Future[QueryResult] =
        this.use(_.sendPreparedStatement(query, values))(executionContext)

    override fun inTransaction[A](f: Connection => Future[A])(implicit context: ExecutionContext = executionContext): Future[A] =
        this.use(_.inTransaction[A](f)(context))(executionContext)
}
