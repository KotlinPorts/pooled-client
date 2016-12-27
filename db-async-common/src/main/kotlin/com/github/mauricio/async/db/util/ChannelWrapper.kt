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

package com.github.mauricio.async.db.util

import com.github.mauricio.async.db.exceptions.UnknownLengthException
import java.nio.charset.Charset
import io.netty.buffer.ByteBuf
import mu.KLogging

//TODO: kotlin implicitConversions?
//TODO: it was AnyVal, is it correct to make it data class?
data class ChannelWrapper(val buffer: ByteBuf) {

    companion object : KLogging() {
        val MySQL_NULL = 0xfb
        fun bufferToWrapper(buffer: ByteBuf) = ChannelWrapper(buffer)
    }

    fun readFixedString(length: Int, charset: Charset): String {
        val bytes = ByteArray(length)
        buffer.readBytes(bytes)
        return String(bytes, charset)
    }

    fun readCString(charset: Charset) = ByteBufferUtils.readCString(buffer, charset)

    fun readUntilEOF(charset: Charset) = ByteBufferUtils.readUntilEOF(buffer, charset)

    fun readLengthEncodedString(charset: Charset): String {
        val length = readBinaryLength()
        return readFixedString(length.toInt(), charset)
    }

    fun readBinaryLength(): Long {
        val firstByte = buffer.readUnsignedByte().toInt()

        if (firstByte <= 250) {
            return firstByte.toLong()
        } else {
            return when (firstByte) {
                MySQL_NULL -> -1L
                252 -> buffer.readUnsignedShort().toLong()
                253 -> readLongInt().toLong()
                254 -> buffer.readLong()
                else -> throw UnknownLengthException(firstByte)
            }
        }
    }

    fun readLongInt(): Int {
        val first = buffer.readByte().toInt()
        val second = buffer.readByte().toInt()
        val third = buffer.readByte().toInt()

        return (first and 0xff) or
                ((second and 0xff) shl 8) or
                ((third and 0xff) shl 16)
    }

    fun writeLength(length: Long) {
        if (length < 251) {
            buffer.writeByte(length.toInt())
        } else if (length < 65536L) {
            buffer.writeByte(252)
            buffer.writeShort(length.toInt())
        } else if (length < 16777216L) {
            buffer.writeByte(253)
            writeLongInt(length.toInt())
        } else {
            buffer.writeByte(254)
            buffer.writeLong(length)
        }
    }

    fun writeLongInt(i: Int) {
        buffer.writeByte(i and 0xff)
        buffer.writeByte(i shl 8)
        buffer.writeByte(i shl 16)
    }

    fun writeLengthEncodedString(value: String, charset: Charset) {
        val bytes = value.toByteArray(charset)
        writeLength(bytes.size.toLong())
        buffer.writeBytes(bytes)
    }

    fun writePacketLength(sequence: Int = 0) {
        ByteBufferUtils.writePacketLength(buffer, sequence)
    }

    fun mysqlReadInt(): Int {
        val first = buffer.readByte().toInt()
        val last = buffer.readByte().toInt()

        return (first and 0xff) or ((last and 0xff) shl 8)
    }


}
