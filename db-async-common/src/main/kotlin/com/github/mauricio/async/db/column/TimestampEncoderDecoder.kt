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

import com.github.mauricio.async.db.exceptions.DateEncoderNotAvailableException
import java.sql.Timestamp
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.time.format.DateTimeFormatterBuilder
import java.time.temporal.TemporalAccessor
import java.util.Calendar
import java.util.Date

open class TimestampEncoderDecoder : ColumnEncoderDecoder {
    companion object {
        val BaseFormat = "yyyy-MM-dd HH:mm:ss"
        val MillisFormat = ".SSSSSS"
        val Instance = TimestampEncoderDecoder()
    }

    private val optional = DateTimeFormatterBuilder()
            .appendPattern(MillisFormat).toFormatter()
    private val optionalTimeZone = DateTimeFormatterBuilder()
            .appendPattern("Z").toFormatter()

    private val builder = DateTimeFormatterBuilder()
            .appendPattern(BaseFormat)
            .appendOptional(optional)
            .appendOptional(optionalTimeZone)

    private val timezonedPrinter = DateTimeFormatterBuilder()
            .appendPattern("${BaseFormat}${MillisFormat}Z").toFormatter()

    private val nonTimezonedPrinter = DateTimeFormatterBuilder()
            .appendPattern("${BaseFormat}${MillisFormat}").toFormatter()

    private val format = builder.toFormatter()

    open val formatter: DateTimeFormatter get() = format

    override fun decode(value: String): Any =
        LocalDateTime.from(formatter.parse(value))

    override fun encode(value: Any): String =
        when(value) {
            is Timestamp -> this.timezonedPrinter.format(value.toInstant())
            is Date -> this.timezonedPrinter.format(value.toInstant())
            is Calendar -> this.timezonedPrinter.format(value.toInstant())
            is TemporalAccessor -> this.timezonedPrinter.format(value)
            else -> throw DateEncoderNotAvailableException (value)
        }

}
