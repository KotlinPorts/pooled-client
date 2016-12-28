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

package com.github.mauricio.async.db.postgresql.util

import java.nio.charset.Charset
import java.security.MessageDigest

object PasswordHelper {

    private final val Lookup = charArrayOf('0', '1', '2', '3', '4', '5', '6', '7', '8', '9',
            'a', 'b', 'c', 'd', 'e', 'f')

    private fun bytesToHex(bytes: ByteArray, hex: ByteArray, offset: Int) {

        var pos = offset
        var i = 0

        while (i < 16) {
            val c = bytes[i].toInt() and 0xff
            var j = c.toInt() shr 4
            hex[pos] = Lookup[j].toByte()
            pos += 1
            j = (c and 0xf)

            hex[pos] = Lookup[j].toByte()
            pos += 1

            i += 1
        }

    }

    fun encode(userText: String, passwordText: String, salt: ByteArray, charset: Charset): ByteArray {
        val user = userText.toByteArray(charset)
        val password = passwordText.toByteArray(charset)

        val md = MessageDigest.getInstance("MD5")

        md.update(password)
        md.update(user)

        val tempDigest = md.digest()

        val hexDigest = ByteArray(35)

        bytesToHex(tempDigest, hexDigest, 0)
        md.update(hexDigest, 0, 32)
        md.update(salt)

        val passDigest = md.digest()

        bytesToHex(passDigest, hexDigest, 3)

        hexDigest[0] = 'm'.toByte()
        hexDigest[1] = 'd'.toByte()
        hexDigest[2] = '5'.toByte()

        return hexDigest
    }

}
