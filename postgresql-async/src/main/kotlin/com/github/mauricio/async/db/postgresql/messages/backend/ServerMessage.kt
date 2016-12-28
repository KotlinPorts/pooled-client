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

package com.github.mauricio.async.db.postgresql.messages.backend

import com.github.mauricio.async.db.KindedMessage

open class ServerMessage(override val kind: Int) : KindedMessage {
    companion object {
        val Authentication = 'R'.toInt()
        val BackendKeyData = 'K'.toInt()
        val Bind = 'B'.toInt()
        val BindComplete = '2'.toInt()
        val CommandComplete = 'C'.toInt()
        val Close = 'X'.toInt()
        val CloseStatementOrPortal = 'C'.toInt()
        val CloseComplete = '3'.toInt()
        val DataRow = 'D'.toInt()
        val Describe = 'D'.toInt()
        val Error = 'E'.toInt()
        val Execute = 'E'.toInt()
        val EmptyQueryString = 'I'.toInt()
        val NoData = 'n'.toInt()
        val Notice = 'N'.toInt()
        val NotificationResponse = 'A'.toInt()
        val ParameterStatus = 'S'.toInt()
        val Parse = 'P'.toInt()
        val ParseComplete = '1'.toInt()
        val PasswordMessage = 'p'.toInt()
        val PortalSuspended = 's'.toInt()
        val Query = 'Q'.toInt()
        val RowDescription = 'T'.toInt()
        val ReadyForQuery = 'Z'.toInt()
        val Sync = 'S'.toInt()
    }
}