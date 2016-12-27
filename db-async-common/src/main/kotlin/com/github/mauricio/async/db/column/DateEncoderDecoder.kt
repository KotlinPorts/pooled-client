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

import java.time.LocalDate
import java.time.format.DateTimeFormatter
import com.github.mauricio.async.db.exceptions.DateEncoderNotAvailableException
import java.time.temporal.TemporalAccessor

object DateEncoderDecoder : ColumnEncoderDecoder {

    private val ZeroedDate = "0000-00-00"

    private val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")

    override fun decode(value: String): LocalDate? =
            if (ZeroedDate == value) {
                null
            } else {
                LocalDate.from(this.formatter.parse(value))
            }

    override fun encode(value: Any): String =
            when (value) {
                is java.sql.Date -> this.formatter.format(value.toLocalDate())
                is TemporalAccessor -> this.formatter.format(value)
                else -> throw DateEncoderNotAvailableException(value)
            }

}
