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
import com.github.mauricio.async.db.exceptions.DateEncoderNotAvailableException
import com.github.mauricio.async.db.general.ColumnData
import com.github.mauricio.async.db.postgresql.messages.backend.PostgreSQLColumnData
import io.netty.buffer.ByteBuf
import mu.KLogging
import java.nio.charset.Charset
import java.sql.Timestamp
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.util.Calendar
import java.util.Date
import java.time.format.DateTimeFormatterBuilder
import java.time.temporal.TemporalAccessor

object PostgreSQLTimestampEncoderDecoder : ColumnEncoderDecoder, KLogging() {

    private val optionalTimeZone = DateTimeFormatterBuilder()
            .appendPattern("Z").toFormatter()

    private val internalFormatters = Array<DateTimeFormatter>(6, {
        index ->
        val ssBuilder = StringBuilder(".")
        for (i in 0..index) {
            ssBuilder.append('S')
        }
        DateTimeFormatterBuilder()
                .appendPattern("yyyy-MM-dd HH:mm:ss")
                .appendPattern(ssBuilder.toString())
                .appendOptional(optionalTimeZone)
                .toFormatter()
    })

    private val internalFormatterWithoutSeconds = DateTimeFormatterBuilder()
            .appendPattern("yyyy-MM-dd HH:mm:ss")
            .appendOptional(optionalTimeZone)
            .toFormatter()

    fun formatter() = internalFormatters[5]

    override fun decode(kind: ColumnData, buffer: ByteBuf, charset: Charset): Any {
        val bytes = ByteArray(buffer.readableBytes())
        buffer.readBytes(bytes)

        val text = String(bytes, charset)

        val columnType = kind as PostgreSQLColumnData

        return LocalDateTime.from(when (columnType.dataType) {
            ColumnTypes.Timestamp,
            ColumnTypes.TimestampArray -> {
                selectFormatter(text).parse(text)
            }
            ColumnTypes.TimestampWithTimezoneArray -> {
                selectFormatter(text).parse(text)
            }
            ColumnTypes.TimestampWithTimezone -> {
                if (columnType.dataTypeModifier > 0) {
                    internalFormatters[columnType.dataTypeModifier - 1].parse(text)
                } else {
                    selectFormatter(text).parse(text)
                }
            }
            else -> throw IllegalArgumentException("Wrong kind of column")
        })
    }

    private fun selectFormatter(text: String) =
            if (text.contains(".")) {
                internalFormatters[5]
            } else {
                internalFormatterWithoutSeconds
            }

    override fun decode(value: String): Any = throw UnsupportedOperationException ("this method should not have been called")

    override fun encode(value: Any): String =
        when (value) {
            is Timestamp -> formatter().format(value.toLocalDateTime())
            is Date -> formatter().format(Timestamp(value.time).toLocalDateTime())
            is Calendar -> formatter().format(Timestamp(value.timeInMillis).toLocalDateTime())
            is TemporalAccessor -> formatter().format(value)
            else -> throw DateEncoderNotAvailableException (value)
        }

    override fun supportsStringDecoding(): Boolean = false

}
