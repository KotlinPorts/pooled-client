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

package com.github.mauricio.async.db.postgresql

import com.github.elizarov.async.async
import com.github.elizarov.async.suspendable
import com.github.mauricio.async.db.QueryResult
import com.github.mauricio.async.db.column.ColumnDecoderRegistry
import com.github.mauricio.async.db.column.ColumnEncoderRegistry
import com.github.mauricio.async.db.exceptions.ConnectionStillRunningQueryException
import com.github.mauricio.async.db.exceptions.InsufficientParametersException
import com.github.mauricio.async.db.general.MutableResultSet
import com.github.mauricio.async.db.postgresql.codec.PostgreSQLConnectionDelegate
import com.github.mauricio.async.db.postgresql.codec.PostgreSQLConnectionHandler
import com.github.mauricio.async.db.postgresql.column.PostgreSQLColumnDecoderRegistry
import com.github.mauricio.async.db.postgresql.column.PostgreSQLColumnEncoderRegistry
import com.github.mauricio.async.db.postgresql.exceptions.*
import com.github.mauricio.async.db.util.*
import com.github.mauricio.async.db.Configuration
import com.github.mauricio.async.db.Connection
import com.github.mauricio.async.db.pool.TimeoutScheduler
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.atomic.AtomicReference

import com.github.mauricio.async.db.postgresql.messages.backend.*
import com.github.mauricio.async.db.postgresql.messages.frontend.*

import io.netty.channel.EventLoopGroup
import java.util.concurrent.CopyOnWriteArrayList

import com.github.mauricio.async.db.postgresql.util.URLParser
import mu.KLogging
import java.time.Duration
import java.util.concurrent.atomic.AtomicBoolean
import kotlin.coroutines.Continuation
import kotlin.coroutines.ContinuationDispatcher
import kotlin.coroutines.suspendCoroutine

class PostgreSQLConnection
(
        val configuration: Configuration = URLParser.DEFAULT,
        val encoderRegistry: ColumnEncoderRegistry = PostgreSQLColumnEncoderRegistry.Instance,
        val decoderRegistry: ColumnDecoderRegistry = PostgreSQLColumnDecoderRegistry.Instance,
        val group: EventLoopGroup = NettyUtils.DefaultEventLoopGroup,
        val executionContext: ContinuationDispatcher = ExecutorServiceUtils.CachedExecutionContext
)
    : PostgreSQLConnectionDelegate,
        Connection, TimeoutScheduler {

    companion object : KLogging() {
        val Counter = AtomicLong()
        val ServerVersionKey = "server_version"
    }

    private val connectionHandler = PostgreSQLConnectionHandler(
            configuration,
            encoderRegistry,
            decoderRegistry,
            this,
            group,
            executionContext
    )

    private val currentCount = Counter.incrementAndGet()
    private val preparedStatementsCounter = AtomicInteger()

    private val parameterStatus = mutableMapOf<String, String>()
    private val parsedStatements = mutableMapOf<String, PreparedStatementHolder>()
    private var authenticated = false

    private var connectionFuture: Continuation<Connection>? = null
    private var connectionFutureFlag = AtomicBoolean(false)

    private var recentError = false
    private val queryPromiseReference = AtomicReference<Continuation<QueryResult>?>(null)
    private var currentQuery: MutableResultSet<PostgreSQLColumnData>? = null
    private var currentPreparedStatement: PreparedStatementHolder? = null
    private var version = Version(0, 0, 0)
    private var notifyListeners = CopyOnWriteArrayList<(NotificationResponse) -> Unit>()

    private var queryResult: QueryResult? = null

    override fun eventLoopGroup() = group

    override fun executionContext() = executionContext

    override val isTimeoutedBool = AtomicBoolean(false)

    fun isReadyForQuery(): Boolean = this.queryPromise() == null

    override suspend fun connect() = suspendCoroutine<Connection> {
        cont ->
        connectionFuture = cont
        async(executionContext) {
            try {
                this.connectionHandler.connect()
            } catch (e: Throwable) {
                connectionFutureFlag.set(true)
                cont.resumeWithException(e)
            }
        }
    }

    override suspend fun disconnect() = suspendable(executionContext) {
        connectionHandler.disconnect()
        this
    }

    override fun onTimeout() {
        async(executionContext) { disconnect() }
    }

    override val isConnected: Boolean get() = this.connectionHandler.isConnected()

    fun parameterStatuses() = this.parameterStatus.toMap()

    override suspend fun sendQuery(query: String): QueryResult {
        validateQuery(query)
        return addTimeout(configuration.queryTimeout) {
            cont ->
            setQueryPromise(cont)
            write(QueryMessage(query))
        }
    }

    override suspend fun sendPreparedStatement(query: String, values: List<Any?>): QueryResult {
        validateQuery(query)
        return addTimeout(configuration.queryTimeout) {
            cont ->
            setQueryPromise(cont)

            val holder = this.parsedStatements.getOrPut(query) {
                PreparedStatementHolder(query, preparedStatementsCounter.incrementAndGet())
            }

            if (holder.paramsCount != values.size) {
                this.clearQueryPromise()
                cont.resumeWithException(InsufficientParametersException(holder.paramsCount, values))
                return@addTimeout
            }

            this.currentPreparedStatement = holder
            this.currentQuery = MutableResultSet(holder.columnDatas)
            write(
                    if (holder.prepared)
                        PreparedStatementExecuteMessage(holder.statementId, holder.realQuery, values, this.encoderRegistry)
                    else {
                        holder.prepared = true
                        PreparedStatementOpeningMessage(holder.statementId, holder.realQuery, values, this.encoderRegistry)
                    })
        }
    }

    override fun onError(exception: Throwable) {
        this.setErrorOnFutures(exception)
    }

    fun hasRecentError(): Boolean = this.recentError

    fun setErrorOnFutures(e: Throwable) {
        this.recentError = true

        logger.error("Error on connection", e)

        val cf = this.connectionFuture
        if (cf != null && !connectionFutureFlag.get()) {
            try {
                connectionFutureFlag.set(true)
                cf.resumeWithException(e)
            } catch (e: Throwable) {
                //nothing, already done
            }
        }
        val cps = this.currentPreparedStatement
        if (cps != null) {
            parsedStatements.remove(cps.query)
        }

        failQueryPromise(e)
    }

    override fun onReadyForQuery() {
        if (!connectionFutureFlag.get()) {
            try {
                connectionFutureFlag.set(true)
                connectionFuture!!.resume(this)
            } catch (e: Throwable) {
            }
        }
        this.recentError = false
        val qr = queryResult
        if (qr != null) {
            succeedQueryPromise(qr)
        }
    }

    override fun onError(message: ErrorMessage) {
        logger.error("Error with message -> {}", message)

        val error = GenericDatabaseException(message)
        error.fillInStackTrace()

        this.setErrorOnFutures(error)
    }

    override fun onCommandComplete(message: CommandCompleteMessage) {
        this.currentPreparedStatement = null
        queryResult = QueryResult(message.rowsAffected.toLong(), message.statusMessage, this.currentQuery)
    }

    override fun onParameterStatus(message: ParameterStatusMessage) {
        this.parameterStatus.put(message.key, message.value)
        if (ServerVersionKey == message.key) {
            this.version = Version(message.value)
        }
    }

    override fun onDataRow(message: DataRowMessage) {
        val items = Array<Any?>(message.values.size, {
            index ->
            val buf = message.values[index]
            if (buf == null) {
                null
            } else {
                try {
                    val columnType = currentQuery!!.columnTypes[index]
                    decoderRegistry.decode(columnType, buf, configuration.charset)
                } finally {
                    buf.release()
                }
            }
        })

        currentQuery!!.addRow(items)
    }

    override fun onRowDescription(message: RowDescriptionMessage) {
        val list = message.columnDatas.toList()
        this.currentQuery = MutableResultSet(list)
        this.setColumnDatas(list)
    }

    private fun setColumnDatas(columnDatas: List<PostgreSQLColumnData>) {
        this.currentPreparedStatement.let {
            if (it != null) {
                it.columnDatas = columnDatas
            }
        }
    }

    override fun onAuthenticationResponse(message: AuthenticationMessage) {
        when (message) {
            is AuthenticationOkMessage -> {
                logger.debug("Successfully logged in to database")
                authenticated = true
            }
            is AuthenticationChallengeCleartextMessage -> {
                write(this.credential(message))
            }
            is AuthenticationChallengeMD5 -> {
                write(this.credential(message))
            }
        }
    }

    override fun onNotificationResponse(message: NotificationResponse) {
        val iterator = this.notifyListeners.iterator()
        while (iterator.hasNext()) {
            iterator.next()(message)
        }
    }

    fun registerNotifyListener(listener: (NotificationResponse) -> Unit) {
        this.notifyListeners.add(listener)
    }

    fun unregisterNotifyListener(listener: (NotificationResponse) -> Unit) {
        this.notifyListeners.remove(listener)
    }

    fun clearNotifyListeners() {
        this.notifyListeners.clear()
    }

    private fun credential(authenticationMessage: AuthenticationChallengeMessage): CredentialMessage =
            if (configuration.password != null) {
                CredentialMessage(
                        configuration.username,
                        configuration.password!!,
                        authenticationMessage.challengeType,
                        authenticationMessage.salt
                )
            } else {
                throw MissingCredentialInformationException(
                        this.configuration.username,
                        this.configuration.password,
                        authenticationMessage.challengeType)
            }

    private fun notReadyForQueryError(errorMessage: String, race: Boolean) {
        logger.error(errorMessage)
        throw ConnectionStillRunningQueryException(
                this.currentCount,
                race
        )
    }

    fun validateIfItIsReadyForQuery(errorMessage: String) {
        if (queryPromise() != null)
            notReadyForQueryError(errorMessage, false)
    }

    private fun validateQuery(query: String) {
        this.validateIfItIsReadyForQuery("Can't run query because there is one query pending already")

        if (query.isEmpty()) {
            throw QueryMustNotBeNullOrEmptyException(query)
        }
    }

    private fun queryPromise(): Continuation<QueryResult>? = queryPromiseReference.get()

    private fun setQueryPromise(promise: Continuation<QueryResult>) {
        if (!this.queryPromiseReference.compareAndSet(null, promise))
            notReadyForQueryError("Can't run query due to a race with another started query", true)
    }

    private fun clearQueryPromise() =
            queryPromiseReference.getAndSet(null)

    private fun failQueryPromise(t: Throwable) {
        clearQueryPromise().let {
            if (it != null) {
                logger.error("Setting error on future $t")
                it.resumeWithException(t)
            }
        }
    }

    private fun succeedQueryPromise(result: QueryResult) {
        this.queryResult = null
        this.currentQuery = null
        clearQueryPromise()?.resume(result)
    }

    private fun write(message: ClientMessage) {
        this.connectionHandler.write(message)
    }

    override fun toString(): String =
            "${this.javaClass.getSimpleName()}{counter=${this.currentCount}}"

    //1.1 errors

    override suspend fun <A> inTransaction(dispatcher: ContinuationDispatcher?,
                                           f: suspend (conn: Connection) -> A) = super.inTransaction(dispatcher, f)

    override suspend fun <T> addTimeout(duration: Duration?, block: (Continuation<T>) -> Unit)
            = super.addTimeout(duration, block)

    override suspend fun <T> addTimeout(durationMillis: Long?, block: (Continuation<T>) -> Unit)
            = super.addTimeout(durationMillis, block)
}
