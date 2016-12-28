/*
 * Copyright 2013 Maurício Linhares
 *
 * Maurício Linhares licenses this file to you under the Apache License,
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

package com.github.mauricio.async.db.postgresql.util

import com.github.mauricio.async.db.postgresql.exceptions.InvalidArrayException
import mu.KLogging

object ArrayStreamingParser : KLogging() {

    fun parse(content: String, delegate: ArrayStreamingParserDelegate) {

        var index = 0
        var escaping = false
        var quoted = false
        var currentElement: StringBuilder? = null
        var opens = 0
        var closes = 0

        while (index < content.length) {
            val char = content[index]

            if (escaping) {
                currentElement!!.append(char)
                escaping = false
            } else {
                if (char == '{' && !quoted) {
                    delegate.arrayStarted()
                    opens += 1

                } else if (char == '}' && !quoted) {
                    if (currentElement != null) {
                        sendElementEvent(currentElement!!, quoted, delegate)
                        currentElement = null
                    }
                    delegate.arrayEnded()
                    closes += 1
                } else
                    when (char) {
                        '"' -> {
                            if (quoted) {
                                sendElementEvent(currentElement!!, quoted, delegate)
                                currentElement = null
                                quoted = false
                            } else {
                                quoted = true
                                currentElement = StringBuilder()
                            }
                        }
                        ',' -> {
                            if (currentElement != null) {
                                sendElementEvent(currentElement!!, quoted, delegate)
                            }
                            currentElement = null
                        }
                        '\\' -> {
                            escaping = true
                        }
                        else -> {
                            if (currentElement == null) {
                                currentElement = StringBuilder()
                            }
                            currentElement.append(char)
                        }
                    }
            }

            index += 1
        }

        if (opens != closes) {
            throw InvalidArrayException("This array is unbalanced %s".format(content))
        }

    }

    fun sendElementEvent(builder: StringBuilder, quoted: Boolean, delegate: ArrayStreamingParserDelegate) {

        val value = builder.toString()

        return if (!quoted && "NULL".equals(value, ignoreCase = true)) {
            delegate.nullElementFound()
        } else {
            delegate.elementFound(value)
        }

    }
}
