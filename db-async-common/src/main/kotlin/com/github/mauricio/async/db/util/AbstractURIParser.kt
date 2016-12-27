/*
 * Copyright 2016 Maurício Linhares
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

import java.net.URI
import java.net.URISyntaxException
import java.net.URLDecoder
import java.nio.charset.Charset

import com.github.mauricio.async.db.exceptions.UnableToParseURLException
import com.github.mauricio.async.db.Configuration
import com.github.mauricio.async.db.SSLConfiguration
import mu.KLogging
import org.funktionale.option.Option
import org.slf4j.LoggerFactory

/**
 * Common parser assisting methods for PG and MySQL URI parsers.
 */
abstract class AbstractURIParser {

    companion object : KLogging() {
        // Constants and value names
        val PORT = "port"
        val DBNAME = "database"
        val HOST = "host"
        val USERNAME = "user"
        val PASSWORD = "password"
    }

    /**
     * Parses out userInfo into a tuple of optional username and password
     *
     * @param userInfo the optional user info string
     * @return a tuple of optional username and password
     */
    protected fun parseUserInfo(userInfo: String?): Pair<String?, String?> {
        //TODO: WTF
        //userInfo?.split(":", false, 2)
        if (userInfo == null || userInfo.isBlank()) return Pair(null, null)
        val k = userInfo.indexOf(':')
        if (k >= 0) {
            return Pair(userInfo.substring(0, k), userInfo.substring(k + 1, userInfo.length))
        } else {
            return Pair(userInfo, null)
        }
    }

    /**
     * A Regex that will match the base name of the driver scheme, minus jdbc:.
     * Eg: postgres(?:ul)?
     */
    abstract protected val SCHEME: Regex

    /**
     * The default for this particular URLParser, ie: appropriate and specific to PG or MySQL accordingly
     */
    abstract val DEFAULT: Configuration


    /**
     * Parses the provided url and returns a Configuration based upon it.  On an error,
     * @param url the URL to parse.
     * @param charset the charset to use.
     * @return a Configuration.
     */
    //    @throws [UnableToParseURLException]("if the URL does not match the expected type, or cannot be parsed for any reason")
    fun parseOrDie(url: String,
                   charset: Charset = DEFAULT.charset): Configuration =
            try {
                val properties = parse(URI(url).parseServerAuthority())

                assembleConfiguration(properties, charset)
            } catch (e: URISyntaxException) {
                throw UnableToParseURLException("Failed to parse URL: $url", e)
            }


    /**
     * Parses the provided url and returns a Configuration based upon it.  On an error,
     * a default configuration is returned.
     * @param url the URL to parse.
     * @param charset the charset to use.
     * @return a Configuration.
     */
    fun parse(url: String,
              charset: Charset = DEFAULT.charset
    ): Configuration =
        try {
            parseOrDie(url, charset)
        } catch (e : Throwable) {
            logger.warn("Connection url '$url' could not be parsed.", e)
            // Fallback to default to maintain current behavior
            DEFAULT
        }

    /**
     * Assembles a configuration out of the provided property map.  This is the generic form, subclasses may override to
     * handle additional properties.
     * @param properties the extracted properties from the URL.
     * @param charset the charset passed in to parse or parseOrDie.
     * @return
     */
    protected fun assembleConfiguration(properties: Map<String, String>, charset: Charset): Configuration =
            DEFAULT.copy(
                    username = properties.getOrDefault(USERNAME, DEFAULT.username),
                    password = properties.get(PASSWORD),
                    database = properties.get(DBNAME),
                    host = properties.getOrDefault(HOST, DEFAULT.host),
                    port = properties.get(PORT)?.let { it.toInt() } ?: DEFAULT.port,
                    ssl = SSLConfiguration(properties),
                    charset = charset
            )


    protected fun parse(uri: URI): Map<String, String> =
            if (SCHEME.matchEntire(uri.scheme) != null) {
                val userInfo = parseUserInfo(uri.getUserInfo())

                val port = uri.port.let { if (it > 0) it else null }
                val db = uri.path?.let {
                    it.drop(it.indexOf("/") + 1).let {
                        if (it.isEmpty()) null else it
                    }
                }
                val host: String? = uri.host

                val builder = mutableMapOf<String, String>()

                userInfo.first?.let { builder[USERNAME] = it }
                userInfo.second?.let { builder[PASSWORD] = it }
                port?.toString()?.let { builder[PORT] = it }
                db?.let { builder[DBNAME] = it }
                host?.let { unwrapIpv6address(it) }?.let { builder[HOST] = it }

                // Parse query string parameters and just append them, overriding anything previously set
                uri?.query?.split('&')?.forEach {
                    parameter ->
                    val (name, value) = parameter.split('=')
                    if (name != null &&
                            value != null) {
                        builder.put(URLDecoder.decode(name, "UTF-8"),
                                URLDecoder.decode(value, "UTF-8"))
                    }
                }

                builder.toMap()
            } else if (uri.scheme == "jdbc") {
                handleJDBC(uri)
            } else throw UnableToParseURLException("Unrecognized URI scheme")

    /**
     * This method breaks out handling of the jdbc: prefixed uri's, allowing them to be handled differently
     * without reimplementing all of parse.
     */
    protected fun handleJDBC(uri: URI): Map<String, String> =
        parse(URI(uri.getSchemeSpecificPart()))

    protected fun unwrapIpv6address(server: String): String =
        if (server.startsWith("[")) {
            server.substring(1, server.length - 1)
        } else server
}

