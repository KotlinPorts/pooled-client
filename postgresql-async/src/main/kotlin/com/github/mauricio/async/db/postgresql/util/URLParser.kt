/**
 *
 */
package com.github.mauricio.async.db.postgresql.util

import java.net.URI
import java.nio.charset.Charset

import com.github.mauricio.async.db.Configuration
import com.github.mauricio.async.db.SSLConfiguration
import com.github.mauricio.async.db.util.AbstractURIParser

/**
 * The PostgreSQL URL parser.
 */

object URLParser : AbstractURIParser() {

    /**
     * The default configuration for PostgreSQL.
     */
    override val DEFAULT = Configuration(
            username = "postgres",
            host = "localhost",
            port = 5432,
            password = null,
            database = null,
            ssl = SSLConfiguration()
    )

    override val SCHEME = "^postgres(?:ql)?$".toRegex()

    private val simplePGDB = "^postgresql:(\\w+)$".toRegex()

    override fun handleJDBC(uri: URI): Map<String, String> = uri.getSchemeSpecificPart().let {
        val matching = simplePGDB.matchEntire(it)
        if (matching != null) mapOf<String, String>(DBNAME to matching.groupValues[1])
        else parse(URI(it))
    }

    /**
     * Assembles a configuration out of the provided property map.  This is the generic form, subclasses may override to
     * handle additional properties.
     *
     * @param properties the extracted properties from the URL.
     * @param charset    the charset passed in to parse or parseOrDie.
     * @return
     */
    override fun assembleConfiguration(properties: Map<String, String>, charset: Charset): Configuration =
            // Add SSL Configuration
            super.assembleConfiguration(properties, charset).copy(
                    ssl = SSLConfiguration(properties)
            )
}
