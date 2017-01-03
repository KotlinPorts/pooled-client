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
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.atomic.AtomicReference

import com.github.mauricio.async.db.postgresql.messages.backend.*
import com.github.mauricio.async.db.postgresql.messages.frontend.*

import io.netty.channel.EventLoopGroup
import java.util.concurrent.CopyOnWriteArrayList

import com.github.mauricio.async.db.postgresql.util.URLParser
import mu.KLogging
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
        Connection {

    companion object : KLogging() {
        val Counter = AtomicLong()
        val ServerVersionKey = "server_version"
    }

    private final val connectionHandler = PostgreSQLConnectionHandler(
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

    private var recentError = false
    private val queryPromiseReference = AtomicReference<Continuation<QueryResult>?>(null)
    private var currentQuery: MutableResultSet<PostgreSQLColumnData>? = null
    private var currentPreparedStatement: PreparedStatementHolder? = null
    private var version = Version(0, 0, 0)
    private var notifyListeners = CopyOnWriteArrayList<(NotificationResponse) -> Unit>()

    private var queryResult: QueryResult? = null

    fun isReadyForQuery(): Boolean = this.queryPromise() == null

    override suspend fun connect() = suspendCoroutine<Connection> {
        cont ->
        connectionFuture = cont
        async(executionContext) {
            try {
                this.connectionHandler.connect()
            } catch (e: Throwable) {
                cont.resumeWithException(e)
            }
        }
    }

    override suspend fun disconnect() = suspendable(executionContext) {
        connectionHandler.disconnect()
        this
    }

    override val isConnected: Boolean get() = this.connectionHandler.isConnected()

    fun parameterStatuses() = this.parameterStatus.toMap()

    override suspend fun sendQuery(query: String): QueryResult = suspendable(executionContext) {
        validateQuery(query)
        try {
            ExecutorServiceUtils.withTimeout(configuration.queryTimeout) {
                suspendCoroutine<QueryResult> {
                    cont ->
                    setQueryPromise(cont)
                    write(QueryMessage(query))
                }
            }
        } catch (e: Throwable) {
            disconnect()
            throw e
        }
    }

    override suspend fun sendPreparedStatement(query: String, values: List<Any?>): QueryResult
            = suspendable(executionContext) {
        validateQuery(query)
        try {
            ExecutorServiceUtils.withTimeout(configuration.queryTimeout) {
                suspendCoroutine<QueryResult> {
                    cont ->
                    setQueryPromise(cont)

                    val holder = this.parsedStatements.getOrPut(query) {
                        PreparedStatementHolder(query, preparedStatementsCounter.incrementAndGet())
                    }

                    if (holder.paramsCount != values.size) {
                        this.clearQueryPromise()
                        cont.resumeWithException(InsufficientParametersException(holder.paramsCount))
                        return@suspendCoroutine
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
        } catch (e: Throwable) {
            disconnect()
            throw e
        }
    }

    fun onError(exception: Throwable) {
        this.setErrorOnFutures(exception)
    }

    fun hasRecentError: Boolean = this.recentError

    private fun setErrorOnFutures(e: Throwable) {
        this.recentError = true

        log.error("Error on connection", e)

        if (!this.connectionFuture.isCompleted) {
            this.connectionFuture.failure(e)
            this.disconnect
        }

        this.currentPreparedStatement.map(p => this.parsedStatements.remove(p.query))
        this.currentPreparedStatement = None
        this.failQueryPromise(e)
    }

    override fun onReadyForQuery() {
        this.connectionFuture.trySuccess(this)

        this.recentError = false
        queryResult.foreach(this.succeedQueryPromise)
    }

    override fun onError(m: ErrorMessage) {
        log.error("Error with message -> {}", m)

        val error = new GenericDatabaseException (m)
        error.fillInStackTrace()

        this.setErrorOnFutures(error)
    }

    override fun onCommandComplete(m: CommandCompleteMessage) {
        this.currentPreparedStatement = None
        queryResult = Some(new QueryResult (m.rowsAffected, m.statusMessage, this.currentQuery))
    }

    override fun onParameterStatus(m: ParameterStatusMessage) {
        this.parameterStatus.put(m.key, m.value)
        if (ServerVersionKey == m.key) {
            this.version = Version(m.value)
        }
    }

    override fun onDataRow(m: DataRowMessage) {
        val items = new Array [ Any](m.values.size)
        var x = 0

        while (x < m.values.size) {
            val buf = m.values(x)
            items(x) = if (buf == null) {
                null
            } else {
                try {
                    val columnType = this.currentQuery.get.columnTypes(x)
                    this.decoderRegistry.decode(columnType, buf, configuration.charset)
                } finally {
                    buf.release()
                }
            }
            x += 1
        }

        this.currentQuery.get.addRow(items)
    }

    override fun onRowDescription(m: RowDescriptionMessage) {
        this.currentQuery = Option(new MutableResultSet (m.columnDatas))
        this.setColumnDatas(m.columnDatas)
    }

    private fun setColumnDatas(columnDatas: Array[PostgreSQLColumnData] )
    {
        this.currentPreparedStatement.foreach {
            holder =>
            holder.columnDatas = columnDatas
        }
    }

    override fun onAuthenticationResponse(message: AuthenticationMessage) {

        message match {
            case m : AuthenticationOkMessage => {
                log.debug("Successfully logged in to database")
                this.authenticated = true
            }
            case m : AuthenticationChallengeCleartextMessage => {
                write(this.credential(m))
            }
            case m : AuthenticationChallengeMD5 => {
                write(this.credential(m))
            }
        }

    }

    override fun onNotificationResponse(message: NotificationResponse) {
        val iterator = this.notifyListeners.iterator()
        while (iterator.hasNext) {
            iterator.next().apply(message)
        }
    }

    fun registerNotifyListener(listener: NotificationResponse => Unit )
    {
        this.notifyListeners.add(listener)
    }

    fun unregisterNotifyListener(listener: NotificationResponse => Unit )
    {
        this.notifyListeners.remove(listener)
    }

    fun clearNotifyListeners() {
        this.notifyListeners.clear()
    }

    private fun credential(authenticationMessage: AuthenticationChallengeMessage): CredentialMessage = {
        if (configuration.username != null && configuration.password.isDefined) {
            new CredentialMessage (
                    configuration.username,
            configuration.password.get,
            authenticationMessage.challengeType,
            authenticationMessage.salt
            )
        } else {
            throw new MissingCredentialInformationException (
                    this.configuration.username,
            this.configuration.password,
            authenticationMessage.challengeType)
        }
    }

    private [this]
    fun notReadyForQueryError(errorMessage: String, race: Boolean) = {
        log.error(errorMessage)
        throw new ConnectionStillRunningQueryException (
                this.currentCount,
        race
        )
    }

    fun validateIfItIsReadyForQuery(errorMessage: String) =
            if (this.queryPromise.isDefined)
                notReadyForQueryError(errorMessage, false)

    private fun validateQuery(query: String) {
        this.validateIfItIsReadyForQuery("Can't run query because there is one query pending already")

        if (query == null || query.isEmpty) {
            throw new QueryMustNotBeNullOrEmptyException (query)
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
}
