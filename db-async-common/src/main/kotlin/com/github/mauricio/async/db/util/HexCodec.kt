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

/**
 * The code from this class was copied from the Hex class at commons-codec
 */

object HexCodec {

    private final val Digits = charArrayOf('0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'A', 'B', 'C', 'D', 'E', 'F')

    private fun toDigit(ch: Char, index: Int): Int {
        val digit = Character.digit(ch, 16)

        if (digit == -1) {
            throw IllegalArgumentException("Illegal hexadecimal character " + ch + " at index " + index);
        }

        return digit
    }

    /**
     *
     * Turns a HEX based char sequence into a Byte array
     *
     * @param value
     * @param start
     * @return
     */

    fun decode(value: CharSequence, start: Int = 0): ByteArray {

        val length = value.length - start
        val end = value.length

        if ((length and 0x01) != 0) {
            throw IllegalArgumentException("Odd number of characters. A hex encoded byte array has to be even.")
        }

        val out = ByteArray(length shr 1)

        var i = 0
        var j = start

        while (j < end) {
            var f = toDigit(value[j], j) shl 4
            j += 1
            f = f or toDigit(value[j], j)
            j += 1
            out[i] = (f and 0xff).toByte()
            i += 1
        }

        return out
    }

    /**
     *
     * Encodes a byte array into a String encoded with Hex values.
     *
     * @param bytes
     * @param prefix
     * @return
     */

    fun encode(bytes: ByteArray, prefix : CharArray? = null ) : String
    {
        val length = (bytes.size * 2) + (prefix?.size?:0)
        val chars = CharArray(length)

        if (prefix != null) {
            var x = 0
            while (x < prefix.size) {
                chars[x] = prefix[x]
                x += 1
            }
        }

        val dataLength = bytes.size
        var j = prefix?.size ?: 0
        var i = 0

        while (i < dataLength) {
            chars[j] = Digits[(0xF0 and bytes[i].toInt()) shr 4]
            j += 1
            chars[j] = Digits[0x0F and bytes[i].toInt()]
            j += 1
            i += 1
        }

        return String (chars)
    }

}
