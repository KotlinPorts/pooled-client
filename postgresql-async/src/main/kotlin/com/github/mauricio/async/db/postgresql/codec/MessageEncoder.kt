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

import com.github.mauricio.async.db.column.ColumnEncoderRegistry
import com.github.mauricio.async.db.exceptions.EncoderNotAvailableException
import com.github.mauricio.async.db.postgresql.encoders.*
import com.github.mauricio.async.db.postgresql.messages.backend.ServerMessage
import com.github.mauricio.async.db.postgresql.messages.frontend.*
import com.github.mauricio.async.db.util.BufferDumper
import java.nio.charset.Charset
import io.netty.handler.codec.MessageToMessageEncoder
import io.netty.channel.ChannelHandlerContext
import mu.KLogging

class MessageEncoder(val charset: Charset, val encoderRegistry: ColumnEncoderRegistry)
    : MessageToMessageEncoder<Any>() {

    companion object : KLogging()

    private val executeEncoder = ExecutePreparedStatementEncoder(charset, encoderRegistry)
    private val openEncoder = PreparedStatementOpeningEncoder(charset, encoderRegistry)
    private val startupEncoder = StartupMessageEncoder(charset)
    private val queryEncoder = QueryMessageEncoder(charset)
    private val credentialEncoder = CredentialEncoder(charset)

    override fun encode(ctx: ChannelHandlerContext, message: Any, out: MutableList<Any>) {
        val buffer = when (message) {
            is SSLRequestMessage -> SSLMessageEncoder.encode()
            is StartupMessage -> startupEncoder.encode(message)
            is ClientMessage -> {
                val encoder = when (message.kind) {
                    ServerMessage.Close -> CloseMessageEncoder
                    ServerMessage.Execute -> this.executeEncoder
                    ServerMessage.Parse -> this.openEncoder
                    ServerMessage.Query -> this.queryEncoder
                    ServerMessage.PasswordMessage -> this.credentialEncoder
                    else -> throw EncoderNotAvailableException(message)
                }

                encoder.encode(message)
            }
            else -> {
                throw IllegalArgumentException("Can not encode message %s".format(message))
            }
        }

//        if (log.isTraceEnabled) {
//            log.trace(s"Sending message ${msg.getClass.getName}\n${BufferDumper.dumpAsHex(buffer)}")
//        }

        out.add(buffer)
    }

}
