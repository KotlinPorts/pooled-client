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

package com.github.mauricio.async.db.postgresql.column

import com.github.mauricio.async.db.column.ColumnDecoder
import com.github.mauricio.async.db.postgresql.util.ArrayStreamingParserDelegate
import com.github.mauricio.async.db.postgresql.util.ArrayStreamingParser
import com.github.mauricio.async.db.general.ColumnData
import io.netty.buffer.Unpooled
import io.netty.buffer.ByteBuf
import org.funktionale.collections.tail
import java.nio.charset.Charset

class ArrayDecoder(private val decoder: ColumnDecoder) : ColumnDecoder {

    override fun decode(kind: ColumnData, buffer: ByteBuf, charset: Charset): List<Any?> {

        val bytes = ByteArray(buffer.readableBytes())
        buffer.readBytes(bytes)
        val value = String(bytes, charset)

        var stack = mutableListOf<MutableList<Any?>>()
        var current: MutableList<Any?>? = null
        var result: MutableList<Any?>? = null
        val delegate = object : ArrayStreamingParserDelegate {
            override fun arrayEnded() {
                result = stack[stack.size - 1]
                stack.removeAt(stack.size - 1)
            }

            override fun elementFound(element: String) {
                val result = if (decoder.supportsStringDecoding()) {
                    decoder.decode(element)
                } else {
                    decoder.decode(kind, Unpooled.wrappedBuffer(element.toByteArray(charset)), charset)
                }
                current!! += result
            }

            override fun nullElementFound() {
                current!!.add(null)
            }

            override fun arrayStarted() {
                current = mutableListOf<Any?>()

                if (stack.isNotEmpty()) {
                    stack[stack.size - 1].add(current)
                }

                stack.add(current!!)
            }
        }

        ArrayStreamingParser.parse(value, delegate)

        return result!!
    }

    override fun decode(value: String): Any? = throw UnsupportedOperationException ("Should not be called")

}
