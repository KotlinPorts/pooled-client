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

import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter

object TimestampWithTimezoneEncoderDecoder : TimestampEncoderDecoder() {

    private val format = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSSZ")

    override val formatter: DateTimeFormatter get() = format

    override fun decode(value: String): Any = {
        ZonedDateTime.from(formatter.parse(value))
    }

}
