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

package com.github.mauricio.async.db.column

import java.time.LocalTime
import java.time.format.DateTimeFormatter
import java.time.format.DateTimeFormatterBuilder
import java.time.temporal.TemporalAccessor

open class TimeEncoderDecoder : ColumnEncoderDecoder {
    companion object {
        val Instance = TimeEncoderDecoder()
    }

    private val optional = DateTimeFormatterBuilder()
            .appendPattern(".SSSSSS").toFormatter()

    private val format = DateTimeFormatterBuilder()
            .appendPattern("HH:mm:ss")
            .appendOptional(optional)
            .toFormatter()

    private val printer = DateTimeFormatterBuilder()
            .appendPattern("HH:mm:ss.SSSSSS")
            .toFormatter()

    open val formatter: DateTimeFormatter get() = format

    override fun decode(value: String): LocalTime =
            LocalTime.from(format.parse(value))

    override fun encode(value: Any): String =
            this.printer.format(value as TemporalAccessor)
}
