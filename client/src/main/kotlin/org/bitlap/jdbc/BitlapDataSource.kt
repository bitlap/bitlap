package org.bitlap.jdbc

import java.io.PrintWriter
import java.sql.Connection
import java.sql.SQLException
import java.util.logging.Logger
import javax.sql.DataSource

/**
 *
 * @author 梦境迷离
 * @since 2021/6/12
 * @version 1.0
 */
class BitlapDataSource : DataSource {
    override fun getLogWriter(): PrintWriter {
        TODO("Not yet implemented")
    }

    override fun setLogWriter(out: PrintWriter?) {
        TODO("Not yet implemented")
    }

    override fun setLoginTimeout(seconds: Int) {
        TODO("Not yet implemented")
    }

    override fun getLoginTimeout(): Int {
        TODO("Not yet implemented")
    }

    override fun getParentLogger(): Logger {
        TODO("Not yet implemented")
    }

    override fun <T : Any?> unwrap(iface: Class<T>?): T {
        TODO("Not yet implemented")
    }

    override fun isWrapperFor(iface: Class<*>?): Boolean {
        TODO("Not yet implemented")
    }

    override fun getConnection(): Connection {
        return try {
            BitlapConnection("", null)
        } catch (ex: Exception) {
            throw SQLException()
        }
    }

    override fun getConnection(username: String?, password: String?): Connection {
        TODO("Not yet implemented")
    }
}
