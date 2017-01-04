package com.github.mauricio.async.db

import java.io.File

/**
 *
 * Contains the SSL configuration necessary to connect to a database.
 *
 * @param mode whether and with what priority a SSL connection will be negotiated, default disabled
 * @param rootCert path to PEM encoded trusted root certificates, None to use internal JDK cacerts, defaults to None
 *
 */
data class SSLConfiguration(val mode: Mode = Mode.Disable, val rootCert: java.io.File? = null) {

    constructor(properties: Map<String, String>) :
            this(
                    modeByValue(properties.getOrDefault("sslmode", "disable")),
                    properties.get("sslrootcert").let { if (it!=null) File(it) else null }
            )

    companion object SSLConfiguration {

        enum class Mode(val value: String) {
            Disable("disable"), // only try a non-SSL connection
            Prefer("prefer"), // first try an SSL connection; if that fails, try a non-SSL connection
            Require("require"), // only try an SSL connection, but don't verify Certificate Authority
            VerifyCA("verify-ca"), // only try an SSL connection, and verify that the server certificate is issued by a trusted certificate authority (CA)
            VerifyFull("verify-full")  // only try an SSL connection, verify that the server certificate is issued by a trusted CA and that the server host name matches that in the certificate
        }

        fun modeByValue(t: String) = Mode.values().find { it.value == t }!!
    }

}