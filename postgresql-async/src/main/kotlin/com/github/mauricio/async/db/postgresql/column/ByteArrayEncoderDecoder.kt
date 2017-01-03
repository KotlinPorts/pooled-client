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

import com.github.mauricio.async.db.column.ColumnEncoderDecoder
import com.github.mauricio.async.db.postgresql.exceptions.ByteArrayFormatNotSupportedException
import com.github.mauricio.async.db.util.HexCodec
import java.nio.ByteBuffer

import io.netty.buffer.ByteBuf
import mu.KLogging

object ByteArrayEncoderDecoder : ColumnEncoderDecoder, KLogging() {

    val HexStart = "\\x"
    val HexStartChars = HexStart.toCharArray()

    override fun decode(value: String): ByteArray =
            if (value.startsWith(HexStart)) {
                HexCodec.decode(value, 2)
            } else {
                // Default encoding is 'escape'

                // Size the buffer to the length of the string, the data can't be bigger
                val buffer = ByteBuffer.allocate(value.length)

                val ci = value.iterator()

                while (ci.hasNext()) {
                    val c = ci.next();
                    when (c) {
                        '\\' -> {
                            val c2 = getCharOrDie(ci)
                            when (c2) {
                                '\\' -> buffer.put('\\'.toByte())
                                else -> {
                                    val firstDigit = c2
                                    val secondDigit = getCharOrDie(ci)
                                    val thirdDigit = getCharOrDie(ci)
                                    // Must always be in triplets
                                    buffer.put(
                                            Integer.decode(
                                                    String(charArrayOf('0', firstDigit, secondDigit, thirdDigit))).toByte())
                                }
                            }
                        }
                        else -> buffer.put(c.toByte())
                    }
                }

                buffer.flip()
                val finalArray = ByteArray(buffer.remaining())
                buffer.get(finalArray)

                finalArray
            }

    /**
     * This is required since {@link Iterator#next} when {@linke Iterator#hasNext} is false is undefined.
     * @param ci the iterator source of the data
     * @return the next character
     * @throws IllegalArgumentException if there is no next character
     */
    private fun getCharOrDie(ci: Iterator<Char>): Char =
            if (ci.hasNext()) {
                ci.next()
            } else {
                throw IllegalArgumentException("Expected escape sequence character, found nothing")
            }

    override fun encode(value: Any): String {
        val array = when (value) {
            is ByteArray -> value

            is ByteBuffer -> if (value.hasArray()) value.array()
            else {
                val arr = ByteArray(value.remaining())
                value.get(arr)
                arr
            }
            is ByteBuf -> if (value.hasArray()) value.array()
            else {
                val arr = ByteArray(value.readableBytes())
                value.getBytes(0, arr)
                arr
            }
            else -> throw IllegalArgumentException("not a byte array/ByteBuffer/ByteArray")
        }

        return HexCodec.encode(array, HexStartChars)
    }

}
