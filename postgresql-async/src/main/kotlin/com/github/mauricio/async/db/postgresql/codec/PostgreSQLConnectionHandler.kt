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

package com.github.mauricio.async.db.postgresql.codec

import com.github.mauricio.async.db.Configuration
import com.github.mauricio.async.db.SSLConfiguration.*
import com.github.mauricio.async.db.column.ColumnDecoderRegistry
import com.github.mauricio.async.db.column.ColumnEncoderRegistry
import com.github.mauricio.async.db.postgresql.exceptions.*
import com.github.mauricio.async.db.postgresql.messages.backend.*
import com.github.mauricio.async.db.postgresql.messages.frontend.*
import com.github.mauricio.async.db.util.*
import java.net.InetSocketAddress
import io.netty.channel.*
import io.netty.bootstrap.Bootstrap
import com.github.mauricio.async.db.postgresql.messages.backend.DataRowMessage
import com.github.mauricio.async.db.postgresql.messages.backend.CommandCompleteMessage
import com.github.mauricio.async.db.postgresql.messages.backend.ProcessData
import com.github.mauricio.async.db.postgresql.messages.backend.RowDescriptionMessage
import com.github.mauricio.async.db.postgresql.messages.backend.ParameterStatusMessage
import io.netty.channel.socket.nio.NioSocketChannel
import io.netty.handler.codec.CodecException
import io.netty.handler.ssl.SslContextBuilder
import io.netty.handler.ssl.SslHandler
import io.netty.handler.ssl.util.InsecureTrustManagerFactory
import io.netty.util.concurrent.FutureListener
import mu.KLogging
import javax.net.ssl.TrustManagerFactory
import java.security.KeyStore
import java.io.FileInputStream
import kotlin.coroutines.Continuation
import kotlin.coroutines.ContinuationDispatcher
import kotlin.coroutines.suspendCoroutine

//TODO: there was implicit toFuture, we'll need somehow convert netty stuff to continuations

class PostgreSQLConnectionHandler
(
        val configuration: Configuration,
        val encoderRegistry: ColumnEncoderRegistry,
        val decoderRegistry: ColumnDecoderRegistry,
        val connectionDelegate: PostgreSQLConnectionDelegate,
        val group: EventLoopGroup,
        val executionService: ContinuationDispatcher
)
    : SimpleChannelInboundHandler<Any>() {
    companion object : KLogging()

    private
    val properties = mapOf(
            "user" to configuration.username,
            "database" to configuration.database,
            "client_encoding" to configuration.charset.name(),
            "DateStyle" to "ISO",
            "extra_float_digits" to "2")

    private val bootstrap = Bootstrap()
    private var connectionContinuation: Continuation<PostgreSQLConnectionHandler>? = null
    private var disconnectionContinuation: Continuation<PostgreSQLConnectionHandler>? = null
    private var processData: ProcessData? = null

    private var currentContext: ChannelHandlerContext? = null

    suspend fun connect(): PostgreSQLConnectionHandler = suspendCoroutine {
        cont ->
        connectionContinuation = cont
        this.bootstrap.group(this.group)
        this.bootstrap.channel(NioSocketChannel::class.java)
        this.bootstrap.handler(object : ChannelInitializer<Channel>() {

            override fun initChannel(ch: Channel) {
                ch.pipeline().addLast(
                        MessageDecoder(configuration.ssl.mode != SSLConfiguration.Mode.Disable, configuration.charset, configuration.maximumMessageSize),
                        MessageEncoder(configuration.charset, encoderRegistry),
                        this@PostgreSQLConnectionHandler)
            }

        })

        this.bootstrap.option<Boolean>(ChannelOption.SO_KEEPALIVE, true)
        this.bootstrap.option(ChannelOption.ALLOCATOR, configuration.allocator)

        this.bootstrap.connect(InetSocketAddress(configuration.host, configuration.port)).addListener {
            channelFuture ->
            if (!channelFuture.isSuccess) {
                connectionContinuation?.resumeWithException(channelFuture.cause())
            }
        }
    }

    suspend fun disconnect(): PostgreSQLConnectionHandler = suspendCoroutine {
        cont ->
        disconnectionContinuation = cont
        if (isConnected()) {
            this.currentContext!!.channel().writeAndFlush(CloseMessage).addClosure {
                writeFuture ->
                if (writeFuture.isSuccess) {
                    writeFuture.channel().close().addClosure {
                        closeFuture ->
                        if (closeFuture.isSuccess) {
                            executionService.dispatchResume(this, disconnectionContinuation!!)
                        } else {
                            executionService.dispatchResumeWithException(closeFuture.cause(), disconnectionContinuation!!)
                        }
                    }
                } else {
                    executionService.dispatchResumeWithException(writeFuture.cause(), disconnectionContinuation!!)
                }
            }
        }
//            case Success (writeFuture) => writeFuture.channel.close().onComplete {
//                case Success (closeFuture) => this.disconnectionPromise.trySuccess(this)
//                case Failure (e) => this.disconnectionPromise.tryFailure(e)
//            }
//            case Failure (e) => this.disconnectionPromise.tryFailure(e)
    }

    fun isConnected(): Boolean =
            currentContext?.channel()?.isActive ?: false

    override fun channelActive(ctx: ChannelHandlerContext) {
        if (configuration.ssl.mode == SSLConfiguration.Mode.Disable)
            ctx.writeAndFlush(StartupMessage(properties))
        else
            ctx.writeAndFlush(SSLRequestMessage)
    }

    override fun channelRead0(ctx: ChannelHandlerContext, msg: Any) {

        when (msg) {
            is SSLResponseMessage ->
                if (msg.supported) {
                    val ctxBuilder = SslContextBuilder.forClient()
                    if (configuration.ssl.mode >= SSLConfiguration.Mode.VerifyCA) {
                        val cert = configuration.ssl.rootCert
                        if (cert == null) {
                            val tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm())
                            val ks = KeyStore.getInstance(KeyStore.getDefaultType())
                            val cacerts = FileInputStream(System.getProperty("java.home") + "/lib/security/cacerts")
                            try {
                                ks.load(cacerts, "changeit".toCharArray())
                            } finally {
                                cacerts.close()
                            }
                            tmf.init(ks)
                            ctxBuilder.trustManager(tmf)
                        } else {
                            ctxBuilder.trustManager(cert)
                        }
                    } else {
                        ctxBuilder.trustManager(InsecureTrustManagerFactory.INSTANCE)
                    }
                    val sslContext = ctxBuilder.build()
                    val sslEngine = sslContext.newEngine(ctx.alloc(), configuration.host, configuration.port)
                    if (configuration.ssl.mode >= SSLConfiguration.Mode.VerifyFull) {
                        val sslParams = sslEngine.getSSLParameters()
                        sslParams.setEndpointIdentificationAlgorithm("HTTPS")
                        sslEngine.setSSLParameters(sslParams)
                    }
                    val handler = SslHandler(sslEngine)
                    ctx.pipeline().addFirst(handler)
                    handler.handshakeFuture().addListener(FutureListener<io.netty.channel.Channel>() {
                        future ->
                        if (future.isSuccess()) {
                            ctx.writeAndFlush(StartupMessage(properties))
                        } else {
                            connectionDelegate.onError(future.cause())
                        }
                    })
                } else if (configuration.ssl.mode < SSLConfiguration.Mode.Require) {
                    ctx.writeAndFlush(StartupMessage(properties))
                } else {
                    connectionDelegate.onError(IllegalArgumentException("SSL is not supported on server"))
                }

            is ServerMessage -> {
                when (msg.kind) {
                    ServerMessage.BackendKeyData -> {
                        this.processData = msg as ProcessData
                    }
                    ServerMessage.BindComplete -> {
                    }
                    ServerMessage.Authentication -> {
                        logger.debug("Authentication response received {}", msg)
                        connectionDelegate.onAuthenticationResponse(msg as AuthenticationMessage)
                    }
                    ServerMessage.CommandComplete -> {
                        connectionDelegate.onCommandComplete(msg as CommandCompleteMessage)
                    }
                    ServerMessage.CloseComplete -> {
                    }
                    ServerMessage.DataRow -> {
                        connectionDelegate.onDataRow(msg as DataRowMessage)
                    }
                    ServerMessage.Error -> {
                        connectionDelegate.onError(msg as ErrorMessage)
                    }
                    ServerMessage.EmptyQueryString -> {
                        val exception = QueryMustNotBeNullOrEmptyException(null)
                        exception.fillInStackTrace()
                        connectionDelegate.onError(exception)
                    }
                    ServerMessage.NoData -> {
                    }
                    ServerMessage.Notice -> {
                        logger.info("Received notice {}", msg)
                    }
                    ServerMessage.NotificationResponse -> {
                        connectionDelegate.onNotificationResponse(msg as NotificationResponse)
                    }
                    ServerMessage.ParameterStatus -> {
                        connectionDelegate.onParameterStatus(msg as ParameterStatusMessage)
                    }
                    ServerMessage.ParseComplete -> {
                    }
                    ServerMessage.ReadyForQuery -> {
                        connectionDelegate.onReadyForQuery()
                    }
                    ServerMessage.RowDescription -> {
                        connectionDelegate.onRowDescription(msg as RowDescriptionMessage)
                    }
                    else -> {
                        val exception = IllegalStateException("Handler not implemented for message %s".format(msg.kind))
                        exception.fillInStackTrace()
                        connectionDelegate.onError(exception)
                    }
                }

            }
            else -> {
                logger.error("Unknown message type - {}", msg)
                val exception = IllegalArgumentException("Unknown message type - %s".format(msg))
                exception.fillInStackTrace()
                connectionDelegate.onError(exception)
            }

        }

    }

    override fun exceptionCaught(ctx: ChannelHandlerContext, cause: Throwable) =
            // unwrap CodecException if needed
            when (cause) {
                is CodecException -> connectionDelegate.onError(cause.cause)
                else -> connectionDelegate.onError(cause)
            }

    override fun channelInactive(ctx: ChannelHandlerContext) {
        logger.info("Connection disconnected - {}", ctx.channel().remoteAddress())
    }

    override fun handlerAdded(ctx: ChannelHandlerContext) {
        this.currentContext = ctx
    }

    fun write(message: ClientMessage) {
        this.currentContext!!.writeAndFlush(message).addClosure {
            future ->
            if (!future.isSuccess) connectionDelegate.onError(future.cause())
        }
    }

}
