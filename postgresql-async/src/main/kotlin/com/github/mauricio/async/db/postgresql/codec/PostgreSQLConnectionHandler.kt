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
import javax.net.ssl.SSLParameters
import javax.net.ssl.TrustManagerFactory
import java.security.KeyStore
import java.io.FileInputStream
import java.util.concurrent.CompletableFuture
import java.util.concurrent.Future
import kotlin.coroutines.Continuation
import kotlin.coroutines.suspendCoroutine

//TODO: there was implicit toFuture, we'll need somehow convert netty stuff to continuations

class PostgreSQLConnectionHandler
(
        val configuration: Configuration,
        val encoderRegistry: ColumnEncoderRegistry,
        val decoderRegistry: ColumnDecoderRegistry,
        val connectionDelegate: PostgreSQLConnectionDelegate,
        val group: EventLoopGroup
//        val executionService: ExecutionService
)
    : SimpleChannelInboundHandler<Any>() {
    companion object : KLogging()

    private
    val properties = listOf(
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
                connectionContinuation = null
            }
        }
    }

    suspend fun disconnect(): PostgreSQLConnectionHandler = suspendCoroutine {
        cont ->
        disconnectionContinuation = cont
        if (isConnected()) {
            this.currentContext!!.channel().writeAndFlush(CloseMessage).addListener {
                writeFuture ->
                if (writeFuture.isSuccess) {
                    writeFuture.channel().close().addHandler {
                        closeFuture ->
                        if (closeFuture.isSuccess) {
                            disconnectionContinuation?.resume(this)
                        }
                    }
                } else {
                    disconnectionContinuation?.resumeWithException(writeFuture.cause())
                }
            }
            case Success (writeFuture) => writeFuture.channel.close().onComplete {
                case Success (closeFuture) => this.disconnectionPromise.trySuccess(this)
                case Failure (e) => this.disconnectionPromise.tryFailure(e)
            }
            case Failure (e) => this.disconnectionPromise.tryFailure(e)
        }
    }
}

fun isConnected: Boolean = {
    if (this.currentContext != null) {
        this.currentContext.channel.isActive
    } else {
        false
    }
}

override fun channelActive(ctx: ChannelHandlerContext): Unit = {
    if (configuration.ssl.mode == Mode.Disable)
        ctx.writeAndFlush(new StartupMessage (this.properties))
    else
        ctx.writeAndFlush(SSLRequestMessage)
}

override fun channelRead0(ctx: ChannelHandlerContext, msg: Object): Unit = {

    msg match {

        case SSLResponseMessage (supported) =>
        if (supported) {
            val ctxBuilder = SslContextBuilder.forClient()
            if (configuration.ssl.mode >= Mode.VerifyCA) {
                configuration.ssl.rootCert.fold {
                    val tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm())
                    val ks = KeyStore.getInstance(KeyStore.getDefaultType())
                    val cacerts = new FileInputStream (System.getProperty("java.home") + "/lib/security/cacerts")
                    try {
                        ks.load(cacerts, "changeit".toCharArray)
                    } finally {
                        cacerts.close()
                    }
                    tmf.init(ks)
                    ctxBuilder.trustManager(tmf)
                } {
                    path =>
                    ctxBuilder.trustManager(path)
                }
            } else {
                ctxBuilder.trustManager(InsecureTrustManagerFactory.INSTANCE)
            }
            val sslContext = ctxBuilder.build()
            val sslEngine = sslContext.newEngine(ctx.alloc(), configuration.host, configuration.port)
            if (configuration.ssl.mode >= Mode.VerifyFull) {
                val sslParams = sslEngine.getSSLParameters()
                sslParams.setEndpointIdentificationAlgorithm("HTTPS")
                sslEngine.setSSLParameters(sslParams)
            }
            val handler = new SslHandler (sslEngine)
            ctx.pipeline().addFirst(handler)
            handler.handshakeFuture.addListener(new FutureListener [ channel . Channel]() {
                fun operationComplete(future: io.netty.util.concurrent.Future[channel.Channel]) {
                if (future.isSuccess()) {
                    ctx.writeAndFlush(new StartupMessage (properties))
                } else {
                    connectionDelegate.onError(future.cause())
                }
            }
            })
        } else if (configuration.ssl.mode < Mode.Require) {
            ctx.writeAndFlush(new StartupMessage (properties))
        } else {
            connectionDelegate.onError(new IllegalArgumentException ("SSL is not supported on server"))
        }

        case m : ServerMessage => {

            (m.kind : @switch) match {
            case ServerMessage . BackendKeyData => {
                this.processData = m.asInstanceOf[ProcessData]
            }
            case ServerMessage . BindComplete => {
            }
            case ServerMessage . Authentication => {
                log.debug("Authentication response received {}", m)
                connectionDelegate.onAuthenticationResponse(m.asInstanceOf[AuthenticationMessage])
            }
            case ServerMessage . CommandComplete => {
                connectionDelegate.onCommandComplete(m.asInstanceOf[CommandCompleteMessage])
            }
            case ServerMessage . CloseComplete => {
            }
            case ServerMessage . DataRow => {
                connectionDelegate.onDataRow(m.asInstanceOf[DataRowMessage])
            }
            case ServerMessage . Error => {
                connectionDelegate.onError(m.asInstanceOf[ErrorMessage])
            }
            case ServerMessage . EmptyQueryString => {
                val exception = new QueryMustNotBeNullOrEmptyException (null)
                exception.fillInStackTrace()
                connectionDelegate.onError(exception)
            }
            case ServerMessage . NoData => {
            }
            case ServerMessage . Notice => {
                log.info("Received notice {}", m)
            }
            case ServerMessage . NotificationResponse => {
                connectionDelegate.onNotificationResponse(m.asInstanceOf[NotificationResponse])
            }
            case ServerMessage . ParameterStatus => {
                connectionDelegate.onParameterStatus(m.asInstanceOf[ParameterStatusMessage])
            }
            case ServerMessage . ParseComplete => {
            }
            case ServerMessage . ReadyForQuery => {
                connectionDelegate.onReadyForQuery()
            }
            case ServerMessage . RowDescription => {
                connectionDelegate.onRowDescription(m.asInstanceOf[RowDescriptionMessage])
            }
            case _ => {
                val exception = new IllegalStateException ("Handler not implemented for message %s".format(m.kind))
                exception.fillInStackTrace()
                connectionDelegate.onError(exception)
            }
        }

        }
        case _ => {
            log.error("Unknown message type - {}", msg)
            val exception = new IllegalArgumentException ("Unknown message type - %s".format(msg))
            exception.fillInStackTrace()
            connectionDelegate.onError(exception)
        }

    }

}

override fun exceptionCaught(ctx: ChannelHandlerContext, cause: Throwable) {
    // unwrap CodecException if needed
    cause match {
        case t : CodecException => connectionDelegate . onError (t.getCause)
        case _ => connectionDelegate . onError (cause)
    }
}

override fun channelInactive(ctx: ChannelHandlerContext): Unit = {
    log.info("Connection disconnected - {}", ctx.channel.remoteAddress)
}

override fun handlerAdded(ctx: ChannelHandlerContext) {
    this.currentContext = ctx
}

fun write(message: ClientMessage) {
    this.currentContext.writeAndFlush(message).onFailure {
        case e : Throwable => connectionDelegate . onError (e)
    }
}

}
