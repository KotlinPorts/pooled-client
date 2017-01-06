/*
 * Copyright 2013 Maurício Linhares
 *
 * Maurício Linhares licenses thColumnTypes.file to you under the Apache License,
 * version 2.0 (the "License"); you may not use thColumnTypes.file except in compliance
 * with the License. You may obtain a copy of the License at:
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License ColumnTypes.distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

package com.github.mauricio.async.db.postgresql.column

import com.github.mauricio.async.db.column.*
import java.nio.charset.Charset
import io.netty.util.CharsetUtil
import io.netty.buffer.ByteBuf
import com.github.mauricio.async.db.general.ColumnData


class PostgreSQLColumnDecoderRegistry(charset: Charset = CharsetUtil.UTF_8) : ColumnDecoderRegistry {

    companion object {
        val Instance = PostgreSQLColumnDecoderRegistry()
    }

    private val stringArrayDecoder = ArrayDecoder(StringEncoderDecoder)
    private val booleanArrayDecoder = ArrayDecoder(BooleanEncoderDecoder)
    private val charArrayDecoder = ArrayDecoder(CharEncoderDecoder)
    private val longArrayDecoder = ArrayDecoder(LongEncoderDecoder)
    private val shortArrayDecoder = ArrayDecoder(ShortEncoderDecoder)
    private val integerArrayDecoder = ArrayDecoder(IntegerEncoderDecoder)
    private val bigDecimalArrayDecoder = ArrayDecoder(BigDecimalEncoderDecoder)
    private val floatArrayDecoder = ArrayDecoder(FloatEncoderDecoder)
    private val doubleArrayDecoder = ArrayDecoder(DoubleEncoderDecoder)
    private val timestampArrayDecoder = ArrayDecoder(PostgreSQLTimestampEncoderDecoder)
    private val timestampWithTimezoneArrayDecoder = ArrayDecoder(PostgreSQLTimestampEncoderDecoder)
    private val dateArrayDecoder = ArrayDecoder(DateEncoderDecoder)
    private val timeArrayDecoder = ArrayDecoder(TimeEncoderDecoder.Instance)
    private val timeWithTimestampArrayDecoder = ArrayDecoder(TimeWithTimezoneEncoderDecoder)
    private val intervalArrayDecoder = ArrayDecoder(PostgreSQLIntervalEncoderDecoder)
    private val uuidArrayDecoder = ArrayDecoder(UUIDEncoderDecoder)
    private val inetAddressArrayDecoder = ArrayDecoder(InetAddressEncoderDecoder)

    override fun decode(kind: ColumnData, value: ByteBuf, charset: Charset): Any? =
        decoderFor(kind.dataType).decode(kind, value, charset)

    fun decoderFor(kind: Int): ColumnDecoder =
            when (kind) {
                ColumnTypes.Boolean -> BooleanEncoderDecoder
                ColumnTypes.BooleanArray -> this.booleanArrayDecoder

                ColumnTypes.Char -> CharEncoderDecoder
                ColumnTypes.CharArray -> this.charArrayDecoder

                ColumnTypes.Bigserial -> LongEncoderDecoder
                ColumnTypes.BigserialArray -> this.longArrayDecoder

                ColumnTypes.Smallint -> ShortEncoderDecoder
                ColumnTypes.SmallintArray -> this.shortArrayDecoder

                ColumnTypes.Integer -> IntegerEncoderDecoder
                ColumnTypes.IntegerArray -> this.integerArrayDecoder

                ColumnTypes.OID -> LongEncoderDecoder
                ColumnTypes.OIDArray -> this.longArrayDecoder

                ColumnTypes.Numeric -> BigDecimalEncoderDecoder
                ColumnTypes.NumericArray -> this.bigDecimalArrayDecoder

                ColumnTypes.Real -> FloatEncoderDecoder
                ColumnTypes.RealArray -> this.floatArrayDecoder

                ColumnTypes.Double -> DoubleEncoderDecoder
                ColumnTypes.DoubleArray -> this.doubleArrayDecoder

                ColumnTypes.Text -> StringEncoderDecoder
                ColumnTypes.TextArray -> this.stringArrayDecoder

                ColumnTypes.Varchar -> StringEncoderDecoder
                ColumnTypes.VarcharArray -> this.stringArrayDecoder

                ColumnTypes.Bpchar -> StringEncoderDecoder
                ColumnTypes.BpcharArray -> this.stringArrayDecoder

                ColumnTypes.Timestamp -> PostgreSQLTimestampEncoderDecoder
                ColumnTypes.TimestampArray -> this.timestampArrayDecoder

                ColumnTypes.TimestampWithTimezone -> PostgreSQLTimestampEncoderDecoder
                ColumnTypes.TimestampWithTimezoneArray -> this.timestampWithTimezoneArrayDecoder

                ColumnTypes.Date -> DateEncoderDecoder
                ColumnTypes.DateArray -> this.dateArrayDecoder

                ColumnTypes.Time -> TimeEncoderDecoder.Instance
                ColumnTypes.TimeArray -> this.timeArrayDecoder

                ColumnTypes.TimeWithTimezone -> TimeWithTimezoneEncoderDecoder
                ColumnTypes.TimeWithTimezoneArray -> this.timeWithTimestampArrayDecoder

                ColumnTypes.Interval -> PostgreSQLIntervalEncoderDecoder
                ColumnTypes.IntervalArray -> this.intervalArrayDecoder

                ColumnTypes.MoneyArray -> this.stringArrayDecoder
                ColumnTypes.NameArray -> this.stringArrayDecoder
                ColumnTypes.UUID -> UUIDEncoderDecoder
                ColumnTypes.UUIDArray -> this.uuidArrayDecoder
                ColumnTypes.XMLArray -> this.stringArrayDecoder
                ColumnTypes.ByteA -> ByteArrayEncoderDecoder

                ColumnTypes.Inet -> InetAddressEncoderDecoder
                ColumnTypes.InetArray -> this.inetAddressArrayDecoder

                else -> StringEncoderDecoder
            }

}
