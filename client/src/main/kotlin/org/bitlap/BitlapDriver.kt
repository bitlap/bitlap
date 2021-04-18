package org.bitlap

import java.sql.Connection
import java.sql.Driver
import java.sql.DriverPropertyInfo
import java.util.Properties
import java.util.logging.Logger

/**
 * Desc: JDBC driver for bitlap
 *
 * Mail: chk19940609@gmail.com
 * Created by IceMimosa
 * Date: 2021/4/16
 */
class BitlapDriver : Driver {
    /**
     * Attempts to make a database connection to the given URL.
     * The driver should return "null" if it realizes it is the wrong kind
     * of driver to connect to the given URL.  This will be common, as when
     * the JDBC driver manager is asked to connect to a given URL it passes
     * the URL to each loaded driver in turn.
     *
     * <P>The driver should throw an `SQLException` if it is the right
     * driver to connect to the given URL but has trouble connecting to
     * the database.
     *
     </P> * <P>The `Properties` argument can be used to pass
     * arbitrary string tag/value pairs as connection arguments.
     * Normally at least "user" and "password" properties should be
     * included in the `Properties` object.
     </P> *
     *
     * <B>Note:</B> If a property is specified as part of the `url` and
     * is also specified in the `Properties` object, it is
     * implementation-defined as to which value will take precedence. For
     * maximum portability, an application should only specify a property once.
     *
     * @param url the URL of the database to which to connect
     * @param info a list of arbitrary string tag/value pairs as
     * connection arguments. Normally at least a "user" and
     * "password" property should be included.
     * @return a `Connection` object that represents a
     * connection to the URL
     * @exception SQLException if a database access error occurs or the url is
     * `null`
     */
    override fun connect(url: String?, info: Properties?): Connection {
        TODO("Not yet implemented")
    }

    /**
     * Retrieves whether the driver thinks that it can open a connection
     * to the given URL.  Typically drivers will return `true` if they
     * understand the sub-protocol specified in the URL and `false` if
     * they do not.
     *
     * @param url the URL of the database
     * @return `true` if this driver understands the given URL;
     * `false` otherwise
     * @exception SQLException if a database access error occurs or the url is
     * `null`
     */
    override fun acceptsURL(url: String?): Boolean {
        TODO("Not yet implemented")
    }

    /**
     * Gets information about the possible properties for this driver.
     * <P>
     * The `getPropertyInfo` method is intended to allow a generic
     * GUI tool to discover what properties it should prompt
     * a human for in order to get
     * enough information to connect to a database.  Note that depending on
     * the values the human has supplied so far, additional values may become
     * necessary, so it may be necessary to iterate though several calls
     * to the `getPropertyInfo` method.
     *
     * @param url the URL of the database to which to connect
     * @param info a proposed list of tag/value pairs that will be sent on
     * connect open
     * @return an array of `DriverPropertyInfo` objects describing
     * possible properties.  This array may be an empty array if
     * no properties are required.
     * @exception SQLException if a database access error occurs
     </P> */
    override fun getPropertyInfo(url: String?, info: Properties?): Array<DriverPropertyInfo> {
        TODO("Not yet implemented")
    }

    /**
     * Retrieves the driver's major version number. Initially this should be 1.
     *
     * @return this driver's major version number
     */
    override fun getMajorVersion(): Int {
        TODO("Not yet implemented")
    }

    /**
     * Gets the driver's minor version number. Initially this should be 0.
     * @return this driver's minor version number
     */
    override fun getMinorVersion(): Int {
        TODO("Not yet implemented")
    }

    /**
     * Reports whether this driver is a genuine JDBC
     * Compliant driver.
     * A driver may only report `true` here if it passes the JDBC
     * compliance tests; otherwise it is required to return `false`.
     * <P>
     * JDBC compliance requires full support for the JDBC API and full support
     * for SQL 92 Entry Level.  It is expected that JDBC compliant drivers will
     * be available for all the major commercial databases.
     </P> * <P>
     * This method is not intended to encourage the development of non-JDBC
     * compliant drivers, but is a recognition of the fact that some vendors
     * are interested in using the JDBC API and framework for lightweight
     * databases that do not support full database functionality, or for
     * special databases such as document information retrieval where a SQL
     * implementation may not be feasible.
     * @return `true` if this driver is JDBC Compliant; `false`
     * otherwise
     </P> */
    override fun jdbcCompliant(): Boolean {
        TODO("Not yet implemented")
    }

    /**
     * Return the parent Logger of all the Loggers used by this driver. This
     * should be the Logger farthest from the root Logger that is
     * still an ancestor of all of the Loggers used by this driver. Configuring
     * this Logger will affect all of the log messages generated by the driver.
     * In the worst case, this may be the root Logger.
     *
     * @return the parent Logger for this driver
     * @throws SQLFeatureNotSupportedException if the driver does not use
     * `java.util.logging`.
     * @since 1.7
     */
    override fun getParentLogger(): Logger {
        TODO("Not yet implemented")
    }
}
