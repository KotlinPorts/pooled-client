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

package com.github.mauricio.async.db.postgresql.parsers

import com.github.mauricio.async.db.exceptions.UnsupportedAuthenticationMethodException
import com.github.mauricio.async.db.postgresql.messages.backend.AuthenticationChallengeMD5
import com.github.mauricio.async.db.postgresql.messages.backend.AuthenticationChallengeCleartextMessage
import com.github.mauricio.async.db.postgresql.messages.backend.AuthenticationOkMessage
import com.github.mauricio.async.db.postgresql.messages.backend.ServerMessage
import io.netty.buffer.ByteBuf

object AuthenticationStartupParser : MessageParser {

    val AuthenticationOk = 0
    val AuthenticationKerberosV5 = 2
    val AuthenticationCleartextPassword = 3
    val AuthenticationMD5Password = 5
    val AuthenticationSCMCredential = 6
    val AuthenticationGSS = 7
    val AuthenticationGSSContinue = 8
    val AuthenticationSSPI = 9

    override fun parseMessage(buffer: ByteBuf): ServerMessage {

        val authenticationType = buffer.readInt()

        return when (authenticationType) {
            AuthenticationOk -> AuthenticationOkMessage.Instance
            AuthenticationCleartextPassword -> AuthenticationChallengeCleartextMessage.Instance
            AuthenticationMD5Password -> {
                val bytes = ByteArray(buffer.readableBytes())
                buffer.readBytes(bytes)
                AuthenticationChallengeMD5(bytes)
            }
            else -> {
                throw UnsupportedAuthenticationMethodException(authenticationType)
            }

        }

    }

}