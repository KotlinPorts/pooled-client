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

package com.github.mauricio.async.db.postgresql.pool

import com.github.elizarov.async.suspendable
import com.github.mauricio.async.db.Configuration
import com.github.mauricio.async.db.exceptions.ConnectionTimeoutedException
import com.github.mauricio.async.db.pool.ObjectFactory
import com.github.mauricio.async.db.postgresql.PostgreSQLConnection
import java.nio.channels.ClosedChannelException
import scala.concurrent.Await
import scala.concurrent.duration._
import scala.language.postfixOps
import scala.concurrent.ExecutionContext
import com.github.mauricio.async.db.util.ExecutorServiceUtils
import com.github.mauricio.async.db.util.NettyUtils
import io.netty.channel.EventLoopGroup
import mu.KLogging
import org.funktionale.either.Either
import kotlin.coroutines.ContinuationDispatcher

/**
 *
 * Object responsible for creating new connection instances.
 *
 * @param configuration
 */

class PostgreSQLConnectionFactory( 
    val configuration : Configuration, 
    val group : EventLoopGroup = NettyUtils.DefaultEventLoopGroup,
    val executionContext : ContinuationDispatcher = ExecutorServiceUtils.CachedExecutionContext ) : ObjectFactory<PostgreSQLConnection> {

  companion object: KLogging()

  override suspend fun create(): PostgreSQLConnection = suspendable(executionContext) {
    val connection = PostgreSQLConnection(configuration, group = group, executionContext = executionContext)
    ExecutorServiceUtils.withTimeout(configuration.connectTimeout) {
      connection.connect()
    }
    connection
  }

  override fun destroy(item: PostgreSQLConnection) {
    item.disconnect()
  }

  /**
   *
   * Validates by checking if the connection is still connected to the database or not.
   *
   * @param item an object produced by this pool
   * @return
   */

  override fun validate( item : PostgreSQLConnection ) : Either<PostgreSQLConnection, Throwable> =
    try {
      if ( item.isTimeouted ) {
        throw ConnectionTimeoutedException(item)
      }
      if ( !item.isConnected || item.hasRecentError ) {
        throw ClosedChannelException()
      } 
      item.validateIfItIsReadyForQuery("Trying to give back a connection that is not ready for query")
      Either.Left(item)
    } catch (e: Throwable) {
      Either.Right(e)
    }

  /**
   *
   * Tests whether we can still send a **SELECT 0** statement to the database.
   *
   * @param item an object produced by this pool
   * @return
   */

  override fun test(item: PostgreSQLConnection): Either<PostgreSQLConnection, Throwable> {
    val result : Try[PostgreSQLConnection] = Try({
      Await.result( item.sendQuery("SELECT 0"), configuration.testTimeout )
      item
    })

    result match {
      case Failure(e) => {
        try {
          if ( item.isConnected ) {
            item.disconnect
          }
        } catch {
          case e : Exception => log.error("Failed disconnecting object", e)
        }
        result
      }
      case Success(i) => {
        result
      }
    }
  }

}
