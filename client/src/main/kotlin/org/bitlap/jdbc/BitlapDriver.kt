package org.bitlap.jdbc

import java.sql.Connection
import java.sql.Driver
import java.sql.DriverPropertyInfo
import java.sql.SQLException
import java.util.Properties
import java.util.logging.Logger
import java.util.regex.Pattern

/**
 * Desc: JDBC driver for bitlap
 *
 * Mail: chk19940609@gmail.com
 * Created by IceMimosa
 * Date: 2021/4/16
 */
class BitlapDriver : Driver {

    companion object {
        val register = java.sql.DriverManager.registerDriver(BitlapDriver())

        /**
         * Major version number of this driver.
         */
        private const val MAJOR_VERSION = 0

        /**
         * Minor version number of this driver.
         */
        private const val MINOR_VERSION = 0

        /**
         * Is this driver JDBC compliant?
         */
        private const val JDBC_COMPLIANT = false

        /**
         * The required prefix for the connection url
         */
        private const val URL_PREFIX = "jdbc:bitlap://"

        /**
         * If host is provided, without a port
         */
        private const val DEFAULT_PORT = "10000"

        /**
         * Property key for the database name
         */
        private const val DBNAME_PROPERTY_KEY = "DBNAME"

        /**
         * Property key for the bitlap Server host
         */
        private const val HOST_PROPERTY_KEY = "HOST"

        /**
         * Property key for the bitlap Server port
         */
        private const val PORT_PROPERTY_KEY = "PORT"

    }

    override fun connect(url: String, info: Properties?): Connection {
        return try {
            BitlapConnection(url, info)
        } catch (ex: Exception) {
            throw SQLException(ex.toString())
        }
    }

    /**
     * Checks whether a given url is in a valid format.
     *
     * The current uri format is:
     * jdbc:bitlap://[host[:port]]
     *
     * jdbc:bitlap://                 - run in embedded mode
     * jdbc:bitlap://localhost        - connect to localhost default port (10000)
     * jdbc:bitlap://localhost:5050   - connect to localhost port 5050
     *
     * TODO: - write a better regex.
     *       - decide on uri format
     */
    override fun acceptsURL(url: String?): Boolean {
        return Pattern.matches("jdbc:bitlap://", url)
    }

    override fun getPropertyInfo(url: String?, info: Properties?): Array<DriverPropertyInfo> {
        var curInfo: Properties = info ?: Properties()
        if (url != null && url.startsWith(URL_PREFIX)) {
            curInfo = parseURL(url, curInfo)
        }

        val hostProp = DriverPropertyInfo(
            HOST_PROPERTY_KEY,
            curInfo.getProperty(HOST_PROPERTY_KEY, "")
        )
        hostProp.required = false
        hostProp.description = "Hostname of Bitlap Server"

        val portProp = DriverPropertyInfo(
            PORT_PROPERTY_KEY,
            curInfo.getProperty(PORT_PROPERTY_KEY, "")
        )
        portProp.required = false
        portProp.description = "Port number of Bitlap Server"

        val dbProp = DriverPropertyInfo(
            DBNAME_PROPERTY_KEY,
            curInfo.getProperty(DBNAME_PROPERTY_KEY, "default")
        )
        dbProp.required = false
        dbProp.description = "Database name"
        return arrayOf(hostProp, portProp, dbProp)
    }

    override fun getMajorVersion(): Int {
        return MAJOR_VERSION
    }

    override fun getMinorVersion(): Int {
        return MINOR_VERSION
    }

    override fun jdbcCompliant(): Boolean {
        return JDBC_COMPLIANT
    }

    override fun getParentLogger(): Logger {
        throw SQLException("Method not supported")
    }

    /**
     * Takes a url in the form of jdbc:bitlap://[hostname]:[port]/[db_name] and parses it.
     * Everything after jdbc:bitlap// is optional.
     *
     * @param url
     * @param defaults
     * @return
     * @throws java.sql.SQLException
     */
    @Throws(SQLException::class)
    private fun parseURL(url: String, defaults: Properties?): Properties {
        val urlProps = if (defaults != null) Properties(defaults) else Properties()
        if (!url.startsWith(URL_PREFIX)) {
            throw SQLException("Invalid connection url: $url")
        }
        if (url.length <= URL_PREFIX.length) return urlProps

        // [hostname]:[port]/[db_name]
        val connectionInfo: String = url.substring(URL_PREFIX.length)

        // [hostname]:[port] [db_name]
        val hostPortAndDatabase = connectionInfo.split("/", limit = 2).toTypedArray()

        // [hostname]:[port]
        if (hostPortAndDatabase[0].isNotEmpty()) {
            val hostAndPort = hostPortAndDatabase[0].split(":", limit = 2).toTypedArray()
            urlProps[HOST_PROPERTY_KEY] = hostAndPort[0]
            if (hostAndPort.size > 1) {
                urlProps[PORT_PROPERTY_KEY] = hostAndPort[1]
            } else {
                urlProps[PORT_PROPERTY_KEY] =
                    DEFAULT_PORT
            }
        }

        // [db_name]
        if (hostPortAndDatabase.size > 1) {
            urlProps[DBNAME_PROPERTY_KEY] = hostPortAndDatabase[1]
        }
        return urlProps
    }
}
