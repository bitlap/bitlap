package org.bitlap.jdbc

import org.bitlap.network.BSQLException
import java.sql.Connection
import java.sql.Driver
import java.sql.DriverPropertyInfo
import java.sql.SQLException
import java.util.Properties
import java.util.logging.Logger

/**
 * Desc: JDBC driver for bitlap
 *
 * Mail: chk19940609@gmail.com
 * Created by IceMimosa
 * Date: 2021/4/16
 */
open class BitlapDriver : Driver {

    companion object {
        init {
            java.sql.DriverManager.registerDriver(BitlapDriver())
        }
    }

    override fun connect(url: String, info: Properties): Connection {
        return try {
            BitlapConnection(url, info)
        } catch (ex: Exception) {
            throw BSQLException(ex.toString())
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
        return url?.startsWith(Utils.URL_PREFIX) ?: false
    }

    override fun getPropertyInfo(url: String?, info: Properties?): Array<DriverPropertyInfo> {
        var curInfo: Properties = info ?: Properties()
        if (url != null && url.startsWith(Utils.URL_PREFIX)) {
            curInfo = parseURL(url, curInfo)
        }

        val hostProp = DriverPropertyInfo(
            Utils.HOST_PROPERTY_KEY,
            curInfo.getProperty(Utils.HOST_PROPERTY_KEY, "")
        )
        hostProp.required = false
        hostProp.description = "Hostname of Bitlap Server"

        val portProp = DriverPropertyInfo(
            Utils.PORT_PROPERTY_KEY,
            curInfo.getProperty(Utils.PORT_PROPERTY_KEY, "")
        )
        portProp.required = false
        portProp.description = "Port number of Bitlap Server"

        val dbProp = DriverPropertyInfo(
            Utils.DBNAME_PROPERTY_KEY,
            curInfo.getProperty(Utils.DBNAME_PROPERTY_KEY, "default")
        )
        dbProp.required = false
        dbProp.description = "Database name"
        return arrayOf(hostProp, portProp, dbProp)
    }

    override fun getMajorVersion(): Int {
        return Utils.MAJOR_VERSION
    }

    override fun getMinorVersion(): Int {
        return Utils.MINOR_VERSION
    }

    override fun jdbcCompliant(): Boolean {
        return Utils.JDBC_COMPLIANT
    }

    override fun getParentLogger(): Logger {
        throw SQLException("Method not supported")
    }

    /**
     * Takes a url in the form of jdbc:bitlap://[hostname1,hostname2]:[port]/[db_name] and parses it.
     *
     * @param url
     * @param defaults
     * @return
     */
    private fun parseURL(url: String, defaults: Properties?): Properties {
        val urlProps = if (defaults != null) Properties(defaults) else Properties()
        if (!url.startsWith(Utils.URL_PREFIX)) {
            throw SQLException("Invalid connection url: $url")
        }
        if (url.length <= Utils.URL_PREFIX.length) return urlProps

        // [hostname]:[port]/[db_name]
        val connectionInfo: String = url.substring(Utils.URL_PREFIX.length)

        // [hostname]:[port]/[db_name]
        val hostPortAndDatabase = connectionInfo.split("/", limit = 2).toTypedArray()

        // [hostname]:[port]
        if (hostPortAndDatabase[0].isNotEmpty()) {
            val hostAndPort = hostPortAndDatabase[0].split(":", limit = 2).toTypedArray()
            urlProps[Utils.HOST_PROPERTY_KEY] = hostAndPort[0]
            if (hostAndPort.size > 1) {
                urlProps[Utils.PORT_PROPERTY_KEY] = hostAndPort[1]
            } else {
                urlProps[Utils.PORT_PROPERTY_KEY] =
                    Utils.DEFAULT_PORT
            }
        }

        // [db_name]
        if (hostPortAndDatabase.size > 1) {
            urlProps[Utils.DBNAME_PROPERTY_KEY] = hostPortAndDatabase[1]
        }
        return urlProps
    }
}
