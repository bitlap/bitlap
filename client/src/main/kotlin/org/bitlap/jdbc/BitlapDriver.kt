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
        throw SQLException("Method not supported")
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
}
