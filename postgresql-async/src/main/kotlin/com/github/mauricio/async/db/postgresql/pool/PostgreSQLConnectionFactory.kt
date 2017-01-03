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

import com.github.elizarov.async.async
import com.github.elizarov.async.suspendable
import com.github.elizarov.async.withTimeout
import com.github.mauricio.async.db.Configuration
import com.github.mauricio.async.db.exceptions.ConnectionTimeoutedException
import com.github.mauricio.async.db.pool.ObjectFactory
import com.github.mauricio.async.db.postgresql.PostgreSQLConnection
import java.nio.channels.ClosedChannelException
import com.github.mauricio.async.db.util.ExecutorServiceUtils
import com.github.mauricio.async.db.util.NettyUtils
import io.netty.channel.EventLoopGroup
import mu.KLogging
import kotlin.coroutines.ContinuationDispatcher

/**
 *
 * Object responsible for creating new connection instances.
 *
 * @param configuration
 */

class PostgreSQLConnectionFactory(
        val configuration: Configuration,
        val group: EventLoopGroup = NettyUtils.DefaultEventLoopGroup,
        val executionContext: ContinuationDispatcher = ExecutorServiceUtils.CachedExecutionContext) : ObjectFactory<PostgreSQLConnection> {

    companion object : KLogging()

    override suspend fun create(): PostgreSQLConnection = suspendable(executionContext) {
        val connection = PostgreSQLConnection(configuration, group = group, executionContext = executionContext)
        withTimeout(group, configuration.connectTimeout) {
            connection.connect()
        }
        connection
    }

    override fun destroy(item: PostgreSQLConnection) {
        async(executionContext) {
            item.disconnect()
        }
    }

    /**
     *
     * Validates by checking if the connection is still connected to the database or not.
     *
     * @param item an object produced by this pool
     * @return
     */

    override suspend fun validate(item: PostgreSQLConnection): PostgreSQLConnection {
        if (item.isTimeouted) {
            throw ConnectionTimeoutedException(item)
        }
        if (!item.isConnected || item.hasRecentError()) {
            throw ClosedChannelException()
        }
        item.validateIfItIsReadyForQuery("Trying to give back a connection that is not ready for query")
        return item
    }

    /**
     *
     * Tests whether we can still send a **SELECT 0** statement to the database.
     *
     * @param item an object produced by this pool
     * @return
     */

    override suspend fun test(item: PostgreSQLConnection) = suspendable(executionContext) {
        try {
            withTimeout(group, configuration.connectTimeout) {
                item.sendQuery("SELECT 0")
            }
            item
        } catch (e: Throwable) {
            try {
                if (item.isConnected) {
                    item.disconnect()
                }
            } catch (e: Throwable) {
                logger.error("Failed disconnecting object", e)
            }
            throw e
        }
    }

}
