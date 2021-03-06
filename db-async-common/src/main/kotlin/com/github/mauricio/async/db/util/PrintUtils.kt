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

package com.github.mauricio.async.db.util

import io.netty.buffer.ByteBuf
import mu.KLogging

object PrintUtils : KLogging() {

    fun printArray(name: String, buffer: ByteBuf) {
        buffer.markReaderIndex()
        val bytes = ByteArray(buffer.readableBytes())
        buffer.readBytes(bytes)
        buffer.resetReaderIndex()
        logger.debug("$name Array[Byte](${bytes.joinToString(", ")})")
    }

}
