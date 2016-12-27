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

import java.time.LocalDateTime
import java.time.format.DateTimeFormatterBuilder
import java.time.temporal.TemporalAccessor

object LocalDateTimeEncoderDecoder : ColumnEncoderDecoder {

    private val ZeroedTimestamp = "0000-00-00 00:00:00"

    private val optional = DateTimeFormatterBuilder()
            .appendPattern(".SSSSSS").toFormatter()

    private val format = DateTimeFormatterBuilder()
            .appendPattern("yyyy-MM-dd HH:mm:ss")
            .appendOptional(optional)
            .toFormatter()

    override fun encode(value: Any): String =
            format.format(value as TemporalAccessor)

    override fun decode(value: String): LocalDateTime? =
            if (ZeroedTimestamp == value) {
                null
            } else {
                LocalDateTime.from(format.parse(value))
            }

}
