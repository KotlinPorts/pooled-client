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

import com.github.mauricio.async.db.column.ColumnEncoder
import com.github.mauricio.async.db.column.ColumnEncoderRegistry
import java.nio.ByteBuffer
import java.time.*

import com.github.mauricio.async.db.column.*
import com.github.mauricio.async.db.postgresql.column.ColumnTypes.Timestamp
import io.netty.buffer.ByteBuf
import java.util.*

class PostgreSQLColumnEncoderRegistry : ColumnEncoderRegistry {

    companion object {
        val Instance = PostgreSQLColumnEncoderRegistry()
    }

    private val classesSequence = listOf<Pair<Class<*>, Pair<ColumnEncoder, Int>>>(
            Int.javaClass to (IntegerEncoderDecoder to ColumnTypes.Numeric),

            Short.javaClass to (ShortEncoderDecoder to ColumnTypes.Numeric),

            Long.javaClass to (LongEncoderDecoder to ColumnTypes.Numeric),

            String.javaClass to (StringEncoderDecoder to ColumnTypes.Varchar),

            Float.javaClass to (FloatEncoderDecoder to ColumnTypes.Numeric),

            Double.javaClass to (DoubleEncoderDecoder to ColumnTypes.Numeric),

            java.math.BigDecimal::class.java to (BigDecimalEncoderDecoder to ColumnTypes.Numeric),

            java.net.InetAddress::class.java to (InetAddressEncoderDecoder to ColumnTypes.Inet),

            java.util.UUID::class.java to (UUIDEncoderDecoder to ColumnTypes.UUID),

            Date::class.java to (TimestampEncoderDecoder.Instance to ColumnTypes.Timestamp),
            Timestamp::class.java to (TimestampEncoderDecoder.Instance to ColumnTypes.Timestamp),

            LocalTime::class.java to (TimeEncoderDecoder.Instance to ColumnTypes.Time),
            LocalDate::class.java to (DateEncoderDecoder to ColumnTypes.Date),
            LocalDateTime::class.java to (TimestampEncoderDecoder.Instance to ColumnTypes.Timestamp),
            ZonedDateTime::class.java to (TimestampWithTimezoneEncoderDecoder to ColumnTypes.TimestampWithTimezone),

            Period::class.java to (PostgreSQLIntervalEncoderDecoder to ColumnTypes.Interval),
            Duration::class.java to (PostgreSQLIntervalEncoderDecoder to ColumnTypes.Interval),

            java.util.Date::class.java to (TimestampWithTimezoneEncoderDecoder to ColumnTypes.TimestampWithTimezone),
            java.sql.Date::class.java to (DateEncoderDecoder to ColumnTypes.Date),
            java.sql.Time::class.java to (SQLTimeEncoder to ColumnTypes.Time),
            java.sql.Timestamp::class.java to (TimestampWithTimezoneEncoderDecoder to ColumnTypes.TimestampWithTimezone),
            java.util.Calendar::class.java to (TimestampWithTimezoneEncoderDecoder to ColumnTypes.TimestampWithTimezone),
            java.util.GregorianCalendar::class.java to (TimestampWithTimezoneEncoderDecoder to ColumnTypes.TimestampWithTimezone),
            ByteArray::class.java to (ByteArrayEncoderDecoder to ColumnTypes.ByteA),
            ByteBuffer::class.java to (ByteArrayEncoderDecoder to ColumnTypes.ByteA),
            ByteBuf::class.java to (ByteArrayEncoderDecoder to ColumnTypes.ByteA)
    )

    private val classes = classesSequence.toMap()

    override fun encode(value: Any?): String? =
            if (value == null) {
                null
            } else encodeValue(value)

    /**
     * Used to encode a value that is not null
     */
    private fun encodeValue(value: Any): String {
        val encoder = this.classes.get(value.javaClass)

        return if (encoder != null) {
            encoder.first.encode(value)
        } else {
            when (value) {
                is Iterable<*> -> encodeCollection(value)
                is Array<*> -> encodeArray(value)
                else -> {
                    val thing = this.classesSequence.find { entry ->
                        entry.first.isAssignableFrom(value.javaClass)
                    }
                    if (thing != null) {
                        thing.second.first.encode(value)
                    } else
                        value.toString()
                }
            }
        }
    }

    //popelyshev
    //TODO: do it more effective, too many object allocations
    private fun encodeCollection(collection: Iterable<*>): String =
            collection.map {
                item ->
                if (item == null) {
                    "NULL"
                } else {
                    if (this.shouldQuote(item)) {
                        "\"" + this.encodeValue(item).replace("\\", """\\""").replace("\"", """\"""") + "\""
                    } else {
                        this.encode(item)
                    }
                }
            }.joinToString(",", "{", "}")

    private fun encodeArray(collection: Array<*>): String =
            collection.map {
                item ->
                if (item == null) {
                    "NULL"
                } else {
                    if (this.shouldQuote(item)) {
                        "\"" + this.encodeValue(item).replace("\\", """\\""").replace("\"", """\"""") + "\""
                    } else {
                        this.encode(item)
                    }
                }
            }.joinToString(",", "{", "}")

    private fun shouldQuote(value: Any): Boolean =
            when (value) {
                is java.lang.Number -> false
                is Int -> false
                is Short -> false
                is Long -> false
                is Float -> false
                is Double -> false
                is java.lang .Iterable<*> -> false
                is Array<*> -> false
                else -> true
            }

    override fun kindOf(value: Any?): Int =
            if (value == null) {
                0
            } else {
                when (value) {
                    is String -> ColumnTypes.Untyped
                    else ->
                        this.classes.get(value.javaClass)?.second ?: ColumnTypes.Untyped
                }
            }

}
