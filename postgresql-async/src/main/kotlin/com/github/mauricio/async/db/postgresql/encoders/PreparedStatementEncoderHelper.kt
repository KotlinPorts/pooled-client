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

package com.github.mauricio.async.db.postgresql.encoders

import com.github.mauricio.async.db.postgresql.messages.backend.ServerMessage
import com.github.mauricio.async.db.util.*
import mu.KLogging
import com.github.mauricio.async.db.column.ColumnEncoderRegistry
import java.nio.charset.Charset
import io.netty.buffer.Unpooled
import io.netty.buffer.ByteBuf

interface PreparedStatementEncoderHelper {

    companion object : KLogging()

    fun writeExecutePortal(
            statementIdBytes: ByteArray,
            query: String,
            values: List<Any?>,
            encoder: ColumnEncoderRegistry,
            charset: Charset,
            writeDescribe: Boolean = false
    ): ByteBuf {

//        if (log.isDebugEnabled) {
//            log.debug(s"Preparing execute portal to statement ($query) - values (${values.mkString(", ")}) - ${charset}")
//        }

        val bindBuffer = Unpooled.buffer(1024)

        bindBuffer.writeByte(ServerMessage.Bind)
        bindBuffer.writeInt(0)

        bindBuffer.writeBytes(statementIdBytes)
        bindBuffer.writeByte(0)
        bindBuffer.writeBytes(statementIdBytes)
        bindBuffer.writeByte(0)

        bindBuffer.writeShort(0)

        bindBuffer.writeShort(values.size)

//        val decodedValues = if (log.isDebugEnabled) {
//            new ArrayBuffer [ String](values.size)
//        } else {
//            null
//        }

        for (value in values) {
            if (value == null) {
                bindBuffer.writeInt(-1)

//                if (log.isDebugEnabled) {
//                    decodedValues += null
//                }
            } else {
                val encodedValue = encoder.encode(value)

//                if (log.isDebugEnabled) {
//                    decodedValues += encodedValue
//                }

                if (encodedValue == null) {
                    bindBuffer.writeInt(-1)
                } else {
                    val content = encodedValue.toByteArray(charset)
                    bindBuffer.writeInt(content.size)
                    bindBuffer.writeBytes(content)
                }

            }
        }

//        if (log.isDebugEnabled) {
//            log.debug(s"Executing portal - statement id (${statementIdBytes.mkString("-")}) - statement ($query) - encoded values (${decodedValues.mkString(", ")}) - original values (${values.mkString(", ")})")
//        }

        bindBuffer.writeShort(0)

        ByteBufferUtils.writeLength(bindBuffer)

        if (writeDescribe) {
            val describeLength = 1 + 4 + 1 + statementIdBytes.size + 1
            val describeBuffer = bindBuffer
            describeBuffer.writeByte(ServerMessage.Describe)
            describeBuffer.writeInt(describeLength - 1)
            describeBuffer.writeByte('P'.toInt())
            describeBuffer.writeBytes(statementIdBytes)
            describeBuffer.writeByte(0)
        }

        val executeLength = 1 + 4 + statementIdBytes.size + 1 + 4
        val executeBuffer = Unpooled.buffer(executeLength)
        executeBuffer.writeByte(ServerMessage.Execute)
        executeBuffer.writeInt(executeLength - 1)
        executeBuffer.writeBytes(statementIdBytes)
        executeBuffer.writeByte(0)
        executeBuffer.writeInt(0)

        val closeLength = 1 + 4 + 1 + statementIdBytes.size + 1
        val closeBuffer = Unpooled.buffer(closeLength)
        closeBuffer.writeByte(ServerMessage.CloseStatementOrPortal)
        closeBuffer.writeInt(closeLength - 1)
        closeBuffer.writeByte('P'.toInt())
        closeBuffer.writeBytes(statementIdBytes)
        closeBuffer.writeByte(0)

        val syncBuffer = Unpooled.buffer(5)
        syncBuffer.writeByte(ServerMessage.Sync)
        syncBuffer.writeInt(4)

        return Unpooled.wrappedBuffer(bindBuffer, executeBuffer, syncBuffer, closeBuffer)

    }
}
