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

abstract class InformationMessage(messageType: Int, val fields: Map<Char, String>)
    : ServerMessage(messageType) {

    companion object {
        val Severity = 'S'
        val SQLState = 'C'
        val Message = 'M'
        val Detail = 'D'
        val Hint = 'H'
        val Position = 'P'
        val InternalQuery = 'q'
        val Where = 'W'
        val File = 'F'
        val Line = 'L'
        val Routine = 'R'

        val Fields = mapOf(
                Severity to "Severity",
                SQLState to "SQLSTATE",
                Message to "Message",
                Detail to "Detail",
                Hint to "Hint",
                Position to "Position",
                InternalQuery to "Internal Query",
                Where to "Where",
                File to "File",
                Line to "Line",
                Routine to "Routine"
        )

        fun fieldName(name: Char): String = Fields.getOrElse(name, {
            name.toString()
        })
    }

    open fun message(): String = fields['M']!!

    override fun toString(): String =
        "%s(fields=%s)".format(this.javaClass.getSimpleName(), fields.map {
            pair -> InformationMessage.fieldName(pair.key) to pair.value
        })

}