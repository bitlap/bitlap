package org.bitlap.jdbc

import java.sql.*

/**
 * Bitlap Statement
 *
 * @author 梦境迷离
 * @since 2021/6/6
 * @version 1.0
 */
class BitlapStatement : Statement {
    /**
     * Returns an object that implements the given interface to allow access to
     * non-standard methods, or standard methods not exposed by the proxy.
     *
     * If the receiver implements the interface then the result is the receiver
     * or a proxy for the receiver. If the receiver is a wrapper
     * and the wrapped object implements the interface then the result is the
     * wrapped object or a proxy for the wrapped object. Otherwise return the
     * the result of calling `unwrap` recursively on the wrapped object
     * or a proxy for that result. If the receiver is not a
     * wrapper and does not implement the interface, then an `SQLException` is thrown.
     *
     * @param <T> the type of the class modeled by this Class object
     * @param iface A Class defining an interface that the result must implement.
     * @return an object that implements the interface. May be a proxy for the actual implementing object.
     * @throws java.sql.SQLException If no object found that implements the interface
     * @since 1.6
    </T> */
    override fun <T : Any?> unwrap(iface: Class<T>?): T {
        throw SQLFeatureNotSupportedException("Method not supported")
    }

    /**
     * Returns true if this either implements the interface argument or is directly or indirectly a wrapper
     * for an object that does. Returns false otherwise. If this implements the interface then return true,
     * else if this is a wrapper then return the result of recursively calling `isWrapperFor` on the wrapped
     * object. If this does not implement the interface and is not a wrapper, return false.
     * This method should be implemented as a low-cost operation compared to `unwrap` so that
     * callers can use this method to avoid expensive `unwrap` calls that may fail. If this method
     * returns true then calling `unwrap` with the same argument should succeed.
     *
     * @param iface a Class defining an interface.
     * @return true if this implements the interface or directly or indirectly wraps an object that does.
     * @throws java.sql.SQLException  if an error occurs while determining whether this is a wrapper
     * for an object with the given interface.
     * @since 1.6
     */
    override fun isWrapperFor(iface: Class<*>?): Boolean {
        throw SQLFeatureNotSupportedException("Method not supported")
    }

    /**
     * Releases this `Statement` object's database
     * and JDBC resources immediately instead of waiting for
     * this to happen when it is automatically closed.
     * It is generally good practice to release resources as soon as
     * you are finished with them to avoid tying up database
     * resources.
     * <P>
     * Calling the method `close` on a `Statement`
     * object that is already closed has no effect.
    </P> * <P>
     * <B>Note:</B>When a `Statement` object is
     * closed, its current `ResultSet` object, if one exists, is
     * also closed.
     *
     * @exception SQLException if a database access error occurs
    </P> */
    override fun close() {
        TODO("Not yet implemented")
    }

    /**
     * Executes the given SQL statement, which returns a single
     * `ResultSet` object.
     *
     *
     * **Note:**This method cannot be called on a
     * `PreparedStatement` or `CallableStatement`.
     * @param sql an SQL statement to be sent to the database, typically a
     * static SQL `SELECT` statement
     * @return a `ResultSet` object that contains the data produced
     * by the given query; never `null`
     * @exception SQLException if a database access error occurs,
     * this method is called on a closed `Statement`, the given
     * SQL statement produces anything other than a single
     * `ResultSet` object, the method is called on a
     * `PreparedStatement` or `CallableStatement`
     * @throws SQLTimeoutException when the driver has determined that the
     * timeout value that was specified by the `setQueryTimeout`
     * method has been exceeded and has at least attempted to cancel
     * the currently running `Statement`
     */
    override fun executeQuery(sql: String?): ResultSet {
        TODO("Not yet implemented")
    }

    /**
     * Executes the given SQL statement, which may be an `INSERT`,
     * `UPDATE`, or `DELETE` statement or an
     * SQL statement that returns nothing, such as an SQL DDL statement.
     *
     *
     * **Note:**This method cannot be called on a
     * `PreparedStatement` or `CallableStatement`.
     * @param sql an SQL Data Manipulation Language (DML) statement, such as `INSERT`, `UPDATE` or
     * `DELETE`; or an SQL statement that returns nothing,
     * such as a DDL statement.
     *
     * @return either (1) the row count for SQL Data Manipulation Language (DML) statements
     * or (2) 0 for SQL statements that return nothing
     *
     * @exception SQLException if a database access error occurs,
     * this method is called on a closed `Statement`, the given
     * SQL statement produces a `ResultSet` object, the method is called on a
     * `PreparedStatement` or `CallableStatement`
     * @throws SQLTimeoutException when the driver has determined that the
     * timeout value that was specified by the `setQueryTimeout`
     * method has been exceeded and has at least attempted to cancel
     * the currently running `Statement`
     */
    override fun executeUpdate(sql: String?): Int {
        throw SQLFeatureNotSupportedException("Method not supported")
    }

    /**
     * Executes the given SQL statement and signals the driver with the
     * given flag about whether the
     * auto-generated keys produced by this `Statement` object
     * should be made available for retrieval.  The driver will ignore the
     * flag if the SQL statement
     * is not an `INSERT` statement, or an SQL statement able to return
     * auto-generated keys (the list of such statements is vendor-specific).
     *
     *
     * **Note:**This method cannot be called on a
     * `PreparedStatement` or `CallableStatement`.
     * @param sql an SQL Data Manipulation Language (DML) statement, such as `INSERT`, `UPDATE` or
     * `DELETE`; or an SQL statement that returns nothing,
     * such as a DDL statement.
     *
     * @param autoGeneratedKeys a flag indicating whether auto-generated keys
     * should be made available for retrieval;
     * one of the following constants:
     * `Statement.RETURN_GENERATED_KEYS`
     * `Statement.NO_GENERATED_KEYS`
     * @return either (1) the row count for SQL Data Manipulation Language (DML) statements
     * or (2) 0 for SQL statements that return nothing
     *
     * @exception SQLException if a database access error occurs,
     * this method is called on a closed `Statement`, the given
     * SQL statement returns a `ResultSet` object,
     * the given constant is not one of those allowed, the method is called on a
     * `PreparedStatement` or `CallableStatement`
     * @exception SQLFeatureNotSupportedException if the JDBC driver does not support
     * this method with a constant of Statement.RETURN_GENERATED_KEYS
     * @throws SQLTimeoutException when the driver has determined that the
     * timeout value that was specified by the `setQueryTimeout`
     * method has been exceeded and has at least attempted to cancel
     * the currently running `Statement`
     * @since 1.4
     */
    override fun executeUpdate(sql: String?, autoGeneratedKeys: Int): Int {
        throw SQLFeatureNotSupportedException("Method not supported")
    }

    /**
     * Executes the given SQL statement and signals the driver that the
     * auto-generated keys indicated in the given array should be made available
     * for retrieval.   This array contains the indexes of the columns in the
     * target table that contain the auto-generated keys that should be made
     * available. The driver will ignore the array if the SQL statement
     * is not an `INSERT` statement, or an SQL statement able to return
     * auto-generated keys (the list of such statements is vendor-specific).
     *
     *
     * **Note:**This method cannot be called on a
     * `PreparedStatement` or `CallableStatement`.
     * @param sql an SQL Data Manipulation Language (DML) statement, such as `INSERT`, `UPDATE` or
     * `DELETE`; or an SQL statement that returns nothing,
     * such as a DDL statement.
     *
     * @param columnIndexes an array of column indexes indicating the columns
     * that should be returned from the inserted row
     * @return either (1) the row count for SQL Data Manipulation Language (DML) statements
     * or (2) 0 for SQL statements that return nothing
     *
     * @exception SQLException if a database access error occurs,
     * this method is called on a closed `Statement`, the SQL
     * statement returns a `ResultSet` object,the second argument
     * supplied to this method is not an
     * `int` array whose elements are valid column indexes, the method is called on a
     * `PreparedStatement` or `CallableStatement`
     * @throws SQLFeatureNotSupportedException  if the JDBC driver does not support this method
     * @throws SQLTimeoutException when the driver has determined that the
     * timeout value that was specified by the `setQueryTimeout`
     * method has been exceeded and has at least attempted to cancel
     * the currently running `Statement`
     * @since 1.4
     */
    override fun executeUpdate(sql: String?, columnIndexes: IntArray?): Int {
        throw SQLFeatureNotSupportedException("Method not supported")
    }

    /**
     * Executes the given SQL statement and signals the driver that the
     * auto-generated keys indicated in the given array should be made available
     * for retrieval.   This array contains the names of the columns in the
     * target table that contain the auto-generated keys that should be made
     * available. The driver will ignore the array if the SQL statement
     * is not an `INSERT` statement, or an SQL statement able to return
     * auto-generated keys (the list of such statements is vendor-specific).
     *
     *
     * **Note:**This method cannot be called on a
     * `PreparedStatement` or `CallableStatement`.
     * @param sql an SQL Data Manipulation Language (DML) statement, such as `INSERT`, `UPDATE` or
     * `DELETE`; or an SQL statement that returns nothing,
     * such as a DDL statement.
     * @param columnNames an array of the names of the columns that should be
     * returned from the inserted row
     * @return either the row count for `INSERT`, `UPDATE`,
     * or `DELETE` statements, or 0 for SQL statements
     * that return nothing
     * @exception SQLException if a database access error occurs,
     * this method is called on a closed `Statement`, the SQL
     * statement returns a `ResultSet` object, the
     * second argument supplied to this method is not a `String` array
     * whose elements are valid column names, the method is called on a
     * `PreparedStatement` or `CallableStatement`
     * @throws SQLFeatureNotSupportedException  if the JDBC driver does not support this method
     * @throws SQLTimeoutException when the driver has determined that the
     * timeout value that was specified by the `setQueryTimeout`
     * method has been exceeded and has at least attempted to cancel
     * the currently running `Statement`
     * @since 1.4
     */
    override fun executeUpdate(sql: String?, columnNames: Array<out String>?): Int {
        throw SQLFeatureNotSupportedException("Method not supported")
    }

    /**
     * Retrieves the maximum number of bytes that can be
     * returned for character and binary column values in a `ResultSet`
     * object produced by this `Statement` object.
     * This limit applies only to  `BINARY`, `VARBINARY`,
     * `LONGVARBINARY`, `CHAR`, `VARCHAR`,
     * `NCHAR`, `NVARCHAR`, `LONGNVARCHAR`
     * and `LONGVARCHAR` columns.  If the limit is exceeded, the
     * excess data is silently discarded.
     *
     * @return the current column size limit for columns storing character and
     * binary values; zero means there is no limit
     * @exception SQLException if a database access error occurs or
     * this method is called on a closed `Statement`
     * @see .setMaxFieldSize
     */
    override fun getMaxFieldSize(): Int {
        throw SQLFeatureNotSupportedException("Method not supported")
    }

    /**
     * Sets the limit for the maximum number of bytes that can be returned for
     * character and binary column values in a `ResultSet`
     * object produced by this `Statement` object.
     *
     * This limit applies
     * only to `BINARY`, `VARBINARY`,
     * `LONGVARBINARY`, `CHAR`, `VARCHAR`,
     * `NCHAR`, `NVARCHAR`, `LONGNVARCHAR` and
     * `LONGVARCHAR` fields.  If the limit is exceeded, the excess data
     * is silently discarded. For maximum portability, use values
     * greater than 256.
     *
     * @param max the new column size limit in bytes; zero means there is no limit
     * @exception SQLException if a database access error occurs,
     * this method is called on a closed `Statement`
     * or the condition `max >= 0` is not satisfied
     * @see .getMaxFieldSize
     */
    override fun setMaxFieldSize(max: Int) {
        throw SQLFeatureNotSupportedException("Method not supported")
    }

    /**
     * Retrieves the maximum number of rows that a
     * `ResultSet` object produced by this
     * `Statement` object can contain.  If this limit is exceeded,
     * the excess rows are silently dropped.
     *
     * @return the current maximum number of rows for a `ResultSet`
     * object produced by this `Statement` object;
     * zero means there is no limit
     * @exception SQLException if a database access error occurs or
     * this method is called on a closed `Statement`
     * @see .setMaxRows
     */
    override fun getMaxRows(): Int {
        throw SQLFeatureNotSupportedException("Method not supported")
    }

    /**
     * Sets the limit for the maximum number of rows that any
     * `ResultSet` object  generated by this `Statement`
     * object can contain to the given number.
     * If the limit is exceeded, the excess
     * rows are silently dropped.
     *
     * @param max the new max rows limit; zero means there is no limit
     * @exception SQLException if a database access error occurs,
     * this method is called on a closed `Statement`
     * or the condition `max >= 0` is not satisfied
     * @see .getMaxRows
     */
    override fun setMaxRows(max: Int) {
        throw SQLFeatureNotSupportedException("Method not supported")
    }

    /**
     * Sets escape processing on or off.
     * If escape scanning is on (the default), the driver will do
     * escape substitution before sending the SQL statement to the database.
     *
     *
     * The `Connection` and `DataSource` property
     * `escapeProcessing` may be used to change the default escape processing
     * behavior.  A value of true (the default) enables escape Processing for
     * all `Statement` objects. A value of false disables escape processing
     * for all `Statement` objects.  The `setEscapeProcessing`
     * method may be used to specify the escape processing behavior for an
     * individual `Statement` object.
     *
     *
     * Note: Since prepared statements have usually been parsed prior
     * to making this call, disabling escape processing for
     * `PreparedStatements` objects will have no effect.
     *
     * @param enable `true` to enable escape processing;
     * `false` to disable it
     * @exception SQLException if a database access error occurs or
     * this method is called on a closed `Statement`
     */
    override fun setEscapeProcessing(enable: Boolean) {
        throw SQLFeatureNotSupportedException("Method not supported")
    }

    /**
     * Retrieves the number of seconds the driver will
     * wait for a `Statement` object to execute.
     * If the limit is exceeded, a
     * `SQLException` is thrown.
     *
     * @return the current query timeout limit in seconds; zero means there is
     * no limit
     * @exception SQLException if a database access error occurs or
     * this method is called on a closed `Statement`
     * @see .setQueryTimeout
     */
    override fun getQueryTimeout(): Int {
        throw SQLFeatureNotSupportedException("Method not supported")
    }

    /**
     * Sets the number of seconds the driver will wait for a
     * `Statement` object to execute to the given number of seconds.
     * By default there is no limit on the amount of time allowed for a running
     * statement to complete. If the limit is exceeded, an
     * `SQLTimeoutException` is thrown.
     * A JDBC driver must apply this limit to the `execute`,
     * `executeQuery` and `executeUpdate` methods.
     *
     *
     * **Note:** JDBC driver implementations may also apply this
     * limit to `ResultSet` methods
     * (consult your driver vendor documentation for details).
     *
     *
     * **Note:** In the case of `Statement` batching, it is
     * implementation defined as to whether the time-out is applied to
     * individual SQL commands added via the `addBatch` method or to
     * the entire batch of SQL commands invoked by the `executeBatch`
     * method (consult your driver vendor documentation for details).
     *
     * @param seconds the new query timeout limit in seconds; zero means
     * there is no limit
     * @exception SQLException if a database access error occurs,
     * this method is called on a closed `Statement`
     * or the condition `seconds >= 0` is not satisfied
     * @see .getQueryTimeout
     */
    override fun setQueryTimeout(seconds: Int) {
        throw SQLFeatureNotSupportedException("Method not supported")
    }

    /**
     * Cancels this `Statement` object if both the DBMS and
     * driver support aborting an SQL statement.
     * This method can be used by one thread to cancel a statement that
     * is being executed by another thread.
     *
     * @exception SQLException if a database access error occurs or
     * this method is called on a closed `Statement`
     * @exception SQLFeatureNotSupportedException if the JDBC driver does not support
     * this method
     */
    override fun cancel() {
        throw SQLFeatureNotSupportedException("Method not supported")
    }

    /**
     * Retrieves the first warning reported by calls on this `Statement` object.
     * Subsequent `Statement` object warnings will be chained to this
     * `SQLWarning` object.
     *
     *
     * The warning chain is automatically cleared each time
     * a statement is (re)executed. This method may not be called on a closed
     * `Statement` object; doing so will cause an `SQLException`
     * to be thrown.
     *
     * <P><B>Note:</B> If you are processing a `ResultSet` object, any
     * warnings associated with reads on that `ResultSet` object
     * will be chained on it rather than on the `Statement`
     * object that produced it.
     *
     * @return the first `SQLWarning` object or `null`
     * if there are no warnings
     * @exception SQLException if a database access error occurs or
     * this method is called on a closed `Statement`
    </P> */
    override fun getWarnings(): SQLWarning {
        throw SQLFeatureNotSupportedException("Method not supported")
    }

    /**
     * Clears all the warnings reported on this `Statement`
     * object. After a call to this method,
     * the method `getWarnings` will return
     * `null` until a new warning is reported for this
     * `Statement` object.
     *
     * @exception SQLException if a database access error occurs or
     * this method is called on a closed `Statement`
     */
    override fun clearWarnings() {
        throw SQLFeatureNotSupportedException("Method not supported")
    }

    /**
     * Sets the SQL cursor name to the given `String`, which
     * will be used by subsequent `Statement` object
     * `execute` methods. This name can then be
     * used in SQL positioned update or delete statements to identify the
     * current row in the `ResultSet` object generated by this
     * statement.  If the database does not support positioned update/delete,
     * this method is a noop.  To insure that a cursor has the proper isolation
     * level to support updates, the cursor's `SELECT` statement
     * should have the form `SELECT FOR UPDATE`.  If
     * `FOR UPDATE` is not present, positioned updates may fail.
     *
     * <P><B>Note:</B> By definition, the execution of positioned updates and
     * deletes must be done by a different `Statement` object than
     * the one that generated the `ResultSet` object being used for
     * positioning. Also, cursor names must be unique within a connection.
     *
     * @param name the new cursor name, which must be unique within
     * a connection
     * @exception SQLException if a database access error occurs or
     * this method is called on a closed `Statement`
     * @throws SQLFeatureNotSupportedException  if the JDBC driver does not support this method
    </P> */
    override fun setCursorName(name: String?) {
        throw SQLFeatureNotSupportedException("Method not supported")
    }

    /**
     * Executes the given SQL statement, which may return multiple results.
     * In some (uncommon) situations, a single SQL statement may return
     * multiple result sets and/or update counts.  Normally you can ignore
     * this unless you are (1) executing a stored procedure that you know may
     * return multiple results or (2) you are dynamically executing an
     * unknown SQL string.
     * <P>
     * The `execute` method executes an SQL statement and indicates the
     * form of the first result.  You must then use the methods
     * `getResultSet` or `getUpdateCount`
     * to retrieve the result, and `getMoreResults` to
     * move to any subsequent result(s).
    </P> *
     *
     * **Note:**This method cannot be called on a
     * `PreparedStatement` or `CallableStatement`.
     * @param sql any SQL statement
     * @return `true` if the first result is a `ResultSet`
     * object; `false` if it is an update count or there are
     * no results
     * @exception SQLException if a database access error occurs,
     * this method is called on a closed `Statement`,
     * the method is called on a
     * `PreparedStatement` or `CallableStatement`
     * @throws SQLTimeoutException when the driver has determined that the
     * timeout value that was specified by the `setQueryTimeout`
     * method has been exceeded and has at least attempted to cancel
     * the currently running `Statement`
     * @see .getResultSet
     *
     * @see .getUpdateCount
     *
     * @see .getMoreResults
     */
    override fun execute(sql: String?): Boolean {
        throw SQLFeatureNotSupportedException("Method not supported")
    }

    /**
     * Executes the given SQL statement, which may return multiple results,
     * and signals the driver that any
     * auto-generated keys should be made available
     * for retrieval.  The driver will ignore this signal if the SQL statement
     * is not an `INSERT` statement, or an SQL statement able to return
     * auto-generated keys (the list of such statements is vendor-specific).
     * <P>
     * In some (uncommon) situations, a single SQL statement may return
     * multiple result sets and/or update counts.  Normally you can ignore
     * this unless you are (1) executing a stored procedure that you know may
     * return multiple results or (2) you are dynamically executing an
     * unknown SQL string.
    </P> * <P>
     * The `execute` method executes an SQL statement and indicates the
     * form of the first result.  You must then use the methods
     * `getResultSet` or `getUpdateCount`
     * to retrieve the result, and `getMoreResults` to
     * move to any subsequent result(s).
    </P> *
     *
     * **Note:**This method cannot be called on a
     * `PreparedStatement` or `CallableStatement`.
     * @param sql any SQL statement
     * @param autoGeneratedKeys a constant indicating whether auto-generated
     * keys should be made available for retrieval using the method
     * `getGeneratedKeys`; one of the following constants:
     * `Statement.RETURN_GENERATED_KEYS` or
     * `Statement.NO_GENERATED_KEYS`
     * @return `true` if the first result is a `ResultSet`
     * object; `false` if it is an update count or there are
     * no results
     * @exception SQLException if a database access error occurs,
     * this method is called on a closed `Statement`, the second
     * parameter supplied to this method is not
     * `Statement.RETURN_GENERATED_KEYS` or
     * `Statement.NO_GENERATED_KEYS`,
     * the method is called on a
     * `PreparedStatement` or `CallableStatement`
     * @exception SQLFeatureNotSupportedException if the JDBC driver does not support
     * this method with a constant of Statement.RETURN_GENERATED_KEYS
     * @throws SQLTimeoutException when the driver has determined that the
     * timeout value that was specified by the `setQueryTimeout`
     * method has been exceeded and has at least attempted to cancel
     * the currently running `Statement`
     * @see .getResultSet
     *
     * @see .getUpdateCount
     *
     * @see .getMoreResults
     *
     * @see .getGeneratedKeys
     *
     *
     * @since 1.4
     */
    override fun execute(sql: String?, autoGeneratedKeys: Int): Boolean {
        throw SQLFeatureNotSupportedException("Method not supported")
    }

    /**
     * Executes the given SQL statement, which may return multiple results,
     * and signals the driver that the
     * auto-generated keys indicated in the given array should be made available
     * for retrieval.  This array contains the indexes of the columns in the
     * target table that contain the auto-generated keys that should be made
     * available.  The driver will ignore the array if the SQL statement
     * is not an `INSERT` statement, or an SQL statement able to return
     * auto-generated keys (the list of such statements is vendor-specific).
     * <P>
     * Under some (uncommon) situations, a single SQL statement may return
     * multiple result sets and/or update counts.  Normally you can ignore
     * this unless you are (1) executing a stored procedure that you know may
     * return multiple results or (2) you are dynamically executing an
     * unknown SQL string.
    </P> * <P>
     * The `execute` method executes an SQL statement and indicates the
     * form of the first result.  You must then use the methods
     * `getResultSet` or `getUpdateCount`
     * to retrieve the result, and `getMoreResults` to
     * move to any subsequent result(s).
    </P> *
     *
     * **Note:**This method cannot be called on a
     * `PreparedStatement` or `CallableStatement`.
     * @param sql any SQL statement
     * @param columnIndexes an array of the indexes of the columns in the
     * inserted row that should be  made available for retrieval by a
     * call to the method `getGeneratedKeys`
     * @return `true` if the first result is a `ResultSet`
     * object; `false` if it is an update count or there
     * are no results
     * @exception SQLException if a database access error occurs,
     * this method is called on a closed `Statement`, the
     * elements in the `int` array passed to this method
     * are not valid column indexes, the method is called on a
     * `PreparedStatement` or `CallableStatement`
     * @throws SQLFeatureNotSupportedException  if the JDBC driver does not support this method
     * @throws SQLTimeoutException when the driver has determined that the
     * timeout value that was specified by the `setQueryTimeout`
     * method has been exceeded and has at least attempted to cancel
     * the currently running `Statement`
     * @see .getResultSet
     *
     * @see .getUpdateCount
     *
     * @see .getMoreResults
     *
     *
     * @since 1.4
     */
    override fun execute(sql: String?, columnIndexes: IntArray?): Boolean {
        throw SQLFeatureNotSupportedException("Method not supported")
    }

    /**
     * Executes the given SQL statement, which may return multiple results,
     * and signals the driver that the
     * auto-generated keys indicated in the given array should be made available
     * for retrieval. This array contains the names of the columns in the
     * target table that contain the auto-generated keys that should be made
     * available.  The driver will ignore the array if the SQL statement
     * is not an `INSERT` statement, or an SQL statement able to return
     * auto-generated keys (the list of such statements is vendor-specific).
     * <P>
     * In some (uncommon) situations, a single SQL statement may return
     * multiple result sets and/or update counts.  Normally you can ignore
     * this unless you are (1) executing a stored procedure that you know may
     * return multiple results or (2) you are dynamically executing an
     * unknown SQL string.
    </P> * <P>
     * The `execute` method executes an SQL statement and indicates the
     * form of the first result.  You must then use the methods
     * `getResultSet` or `getUpdateCount`
     * to retrieve the result, and `getMoreResults` to
     * move to any subsequent result(s).
    </P> *
     *
     * **Note:**This method cannot be called on a
     * `PreparedStatement` or `CallableStatement`.
     * @param sql any SQL statement
     * @param columnNames an array of the names of the columns in the inserted
     * row that should be made available for retrieval by a call to the
     * method `getGeneratedKeys`
     * @return `true` if the next result is a `ResultSet`
     * object; `false` if it is an update count or there
     * are no more results
     * @exception SQLException if a database access error occurs,
     * this method is called on a closed `Statement`,the
     * elements of the `String` array passed to this
     * method are not valid column names, the method is called on a
     * `PreparedStatement` or `CallableStatement`
     * @throws SQLFeatureNotSupportedException  if the JDBC driver does not support this method
     * @throws SQLTimeoutException when the driver has determined that the
     * timeout value that was specified by the `setQueryTimeout`
     * method has been exceeded and has at least attempted to cancel
     * the currently running `Statement`
     * @see .getResultSet
     *
     * @see .getUpdateCount
     *
     * @see .getMoreResults
     *
     * @see .getGeneratedKeys
     *
     *
     * @since 1.4
     */
    override fun execute(sql: String?, columnNames: Array<out String>?): Boolean {
        throw SQLFeatureNotSupportedException("Method not supported")
    }

    /**
     * Retrieves the current result as a `ResultSet` object.
     * This method should be called only once per result.
     *
     * @return the current result as a `ResultSet` object or
     * `null` if the result is an update count or there are no more results
     * @exception SQLException if a database access error occurs or
     * this method is called on a closed `Statement`
     * @see .execute
     */
    override fun getResultSet(): ResultSet {
        TODO("Not yet implemented")
    }

    /**
     * Retrieves the current result as an update count;
     * if the result is a `ResultSet` object or there are no more results, -1
     * is returned. This method should be called only once per result.
     *
     * @return the current result as an update count; -1 if the current result is a
     * `ResultSet` object or there are no more results
     * @exception SQLException if a database access error occurs or
     * this method is called on a closed `Statement`
     * @see .execute
     */
    override fun getUpdateCount(): Int {
        throw SQLFeatureNotSupportedException("Method not supported")
    }

    /**
     * Moves to this `Statement` object's next result, returns
     * `true` if it is a `ResultSet` object, and
     * implicitly closes any current `ResultSet`
     * object(s) obtained with the method `getResultSet`.
     *
     * <P>There are no more results when the following is true:
    </P> * <PRE>`// stmt is a Statement object
     * ((stmt.getMoreResults() == false) && (stmt.getUpdateCount() == -1))
    `</PRE> *
     *
     * @return `true` if the next result is a `ResultSet`
     * object; `false` if it is an update count or there are
     * no more results
     * @exception SQLException if a database access error occurs or
     * this method is called on a closed `Statement`
     * @see .execute
     */
    override fun getMoreResults(): Boolean {
        throw SQLFeatureNotSupportedException("Method not supported")
    }

    /**
     * Moves to this `Statement` object's next result, deals with
     * any current `ResultSet` object(s) according  to the instructions
     * specified by the given flag, and returns
     * `true` if the next result is a `ResultSet` object.
     *
     * <P>There are no more results when the following is true:
    </P> * <PRE>`// stmt is a Statement object
     * ((stmt.getMoreResults(current) == false) && (stmt.getUpdateCount() == -1))
    `</PRE> *
     *
     * @param current one of the following `Statement`
     * constants indicating what should happen to current
     * `ResultSet` objects obtained using the method
     * `getResultSet`:
     * `Statement.CLOSE_CURRENT_RESULT`,
     * `Statement.KEEP_CURRENT_RESULT`, or
     * `Statement.CLOSE_ALL_RESULTS`
     * @return `true` if the next result is a `ResultSet`
     * object; `false` if it is an update count or there are no
     * more results
     * @exception SQLException if a database access error occurs,
     * this method is called on a closed `Statement` or the argument
     * supplied is not one of the following:
     * `Statement.CLOSE_CURRENT_RESULT`,
     * `Statement.KEEP_CURRENT_RESULT` or
     * `Statement.CLOSE_ALL_RESULTS`
     * @exception SQLFeatureNotSupportedException if
     * `DatabaseMetaData.supportsMultipleOpenResults` returns
     * `false` and either
     * `Statement.KEEP_CURRENT_RESULT` or
     * `Statement.CLOSE_ALL_RESULTS` are supplied as
     * the argument.
     * @since 1.4
     * @see .execute
     */
    override fun getMoreResults(current: Int): Boolean {
        throw SQLFeatureNotSupportedException("Method not supported")
    }

    /**
     * Gives the driver a hint as to the direction in which
     * rows will be processed in `ResultSet`
     * objects created using this `Statement` object.  The
     * default value is `ResultSet.FETCH_FORWARD`.
     * <P>
     * Note that this method sets the default fetch direction for
     * result sets generated by this `Statement` object.
     * Each result set has its own methods for getting and setting
     * its own fetch direction.
     *
     * @param direction the initial direction for processing rows
     * @exception SQLException if a database access error occurs,
     * this method is called on a closed `Statement`
     * or the given direction
     * is not one of `ResultSet.FETCH_FORWARD`,
     * `ResultSet.FETCH_REVERSE`, or `ResultSet.FETCH_UNKNOWN`
     * @since 1.2
     * @see .getFetchDirection
    </P> */
    override fun setFetchDirection(direction: Int) {
        throw SQLFeatureNotSupportedException("Method not supported")
    }

    /**
     * Retrieves the direction for fetching rows from
     * database tables that is the default for result sets
     * generated from this `Statement` object.
     * If this `Statement` object has not set
     * a fetch direction by calling the method `setFetchDirection`,
     * the return value is implementation-specific.
     *
     * @return the default fetch direction for result sets generated
     * from this `Statement` object
     * @exception SQLException if a database access error occurs or
     * this method is called on a closed `Statement`
     * @since 1.2
     * @see .setFetchDirection
     */
    override fun getFetchDirection(): Int {
        throw SQLFeatureNotSupportedException("Method not supported")
    }

    /**
     * Gives the JDBC driver a hint as to the number of rows that should
     * be fetched from the database when more rows are needed for
     * `ResultSet` objects generated by this `Statement`.
     * If the value specified is zero, then the hint is ignored.
     * The default value is zero.
     *
     * @param rows the number of rows to fetch
     * @exception SQLException if a database access error occurs,
     * this method is called on a closed `Statement` or the
     * condition `rows >= 0` is not satisfied.
     * @since 1.2
     * @see .getFetchSize
     */
    override fun setFetchSize(rows: Int) {
        throw SQLFeatureNotSupportedException("Method not supported")
    }

    /**
     * Retrieves the number of result set rows that is the default
     * fetch size for `ResultSet` objects
     * generated from this `Statement` object.
     * If this `Statement` object has not set
     * a fetch size by calling the method `setFetchSize`,
     * the return value is implementation-specific.
     *
     * @return the default fetch size for result sets generated
     * from this `Statement` object
     * @exception SQLException if a database access error occurs or
     * this method is called on a closed `Statement`
     * @since 1.2
     * @see .setFetchSize
     */
    override fun getFetchSize(): Int {
        throw SQLFeatureNotSupportedException("Method not supported")
    }

    /**
     * Retrieves the result set concurrency for `ResultSet` objects
     * generated by this `Statement` object.
     *
     * @return either `ResultSet.CONCUR_READ_ONLY` or
     * `ResultSet.CONCUR_UPDATABLE`
     * @exception SQLException if a database access error occurs or
     * this method is called on a closed `Statement`
     * @since 1.2
     */
    override fun getResultSetConcurrency(): Int {
        throw SQLFeatureNotSupportedException("Method not supported")
    }

    /**
     * Retrieves the result set type for `ResultSet` objects
     * generated by this `Statement` object.
     *
     * @return one of `ResultSet.TYPE_FORWARD_ONLY`,
     * `ResultSet.TYPE_SCROLL_INSENSITIVE`, or
     * `ResultSet.TYPE_SCROLL_SENSITIVE`
     * @exception SQLException if a database access error occurs or
     * this method is called on a closed `Statement`
     * @since 1.2
     */
    override fun getResultSetType(): Int {
        throw SQLFeatureNotSupportedException("Method not supported")
    }

    /**
     * Adds the given SQL command to the current list of commands for this
     * `Statement` object. The commands in this list can be
     * executed as a batch by calling the method `executeBatch`.
     * <P>
     * **Note:**This method cannot be called on a
     * `PreparedStatement` or `CallableStatement`.
     * @param sql typically this is a SQL `INSERT` or
     * `UPDATE` statement
     * @exception SQLException if a database access error occurs,
     * this method is called on a closed `Statement`, the
     * driver does not support batch updates, the method is called on a
     * `PreparedStatement` or `CallableStatement`
     * @see .executeBatch
     *
     * @see DatabaseMetaData.supportsBatchUpdates
     *
     * @since 1.2
    </P> */
    override fun addBatch(sql: String?) {
        throw SQLFeatureNotSupportedException("Method not supported")
    }

    /**
     * Empties this `Statement` object's current list of
     * SQL commands.
     * <P>
     * @exception SQLException if a database access error occurs,
     * this method is called on a closed `Statement` or the
     * driver does not support batch updates
     * @see .addBatch
     *
     * @see DatabaseMetaData.supportsBatchUpdates
     *
     * @since 1.2
    </P> */
    override fun clearBatch() {
        throw SQLFeatureNotSupportedException("Method not supported")
    }

    /**
     * Submits a batch of commands to the database for execution and
     * if all commands execute successfully, returns an array of update counts.
     * The `int` elements of the array that is returned are ordered
     * to correspond to the commands in the batch, which are ordered
     * according to the order in which they were added to the batch.
     * The elements in the array returned by the method `executeBatch`
     * may be one of the following:
     * <OL>
     * <LI>A number greater than or equal to zero -- indicates that the
     * command was processed successfully and is an update count giving the
     * number of rows in the database that were affected by the command's
     * execution
    </LI> * <LI>A value of `SUCCESS_NO_INFO` -- indicates that the command was
     * processed successfully but that the number of rows affected is
     * unknown
     * <P>
     * If one of the commands in a batch update fails to execute properly,
     * this method throws a `BatchUpdateException`, and a JDBC
     * driver may or may not continue to process the remaining commands in
     * the batch.  However, the driver's behavior must be consistent with a
     * particular DBMS, either always continuing to process commands or never
     * continuing to process commands.  If the driver continues processing
     * after a failure, the array returned by the method
     * `BatchUpdateException.getUpdateCounts`
     * will contain as many elements as there are commands in the batch, and
     * at least one of the elements will be the following:
     *
    </P></LI> * <LI>A value of `EXECUTE_FAILED` -- indicates that the command failed
     * to execute successfully and occurs only if a driver continues to
     * process commands after a command fails
    </LI></OL> *
     * <P>
     * The possible implementations and return values have been modified in
     * the Java 2 SDK, Standard Edition, version 1.3 to
     * accommodate the option of continuing to process commands in a batch
     * update after a `BatchUpdateException` object has been thrown.
     *
     * @return an array of update counts containing one element for each
     * command in the batch.  The elements of the array are ordered according
     * to the order in which commands were added to the batch.
     * @exception SQLException if a database access error occurs,
     * this method is called on a closed `Statement` or the
     * driver does not support batch statements. Throws [BatchUpdateException]
     * (a subclass of `SQLException`) if one of the commands sent to the
     * database fails to execute properly or attempts to return a result set.
     * @throws SQLTimeoutException when the driver has determined that the
     * timeout value that was specified by the `setQueryTimeout`
     * method has been exceeded and has at least attempted to cancel
     * the currently running `Statement`
     *
     * @see .addBatch
     *
     * @see DatabaseMetaData.supportsBatchUpdates
     *
     * @since 1.2
    </P> */
    override fun executeBatch(): IntArray {
        throw SQLFeatureNotSupportedException("Method not supported")
    }

    /**
     * Retrieves the `Connection` object
     * that produced this `Statement` object.
     * @return the connection that produced this statement
     * @exception SQLException if a database access error occurs or
     * this method is called on a closed `Statement`
     * @since 1.2
     */
    override fun getConnection(): Connection {
        TODO("Not yet implemented")
    }

    /**
     * Retrieves any auto-generated keys created as a result of executing this
     * `Statement` object. If this `Statement` object did
     * not generate any keys, an empty `ResultSet`
     * object is returned.
     *
     *
     * <B>Note:</B>If the columns which represent the auto-generated keys were not specified,
     * the JDBC driver implementation will determine the columns which best represent the auto-generated keys.
     *
     * @return a `ResultSet` object containing the auto-generated key(s)
     * generated by the execution of this `Statement` object
     * @exception SQLException if a database access error occurs or
     * this method is called on a closed `Statement`
     * @throws SQLFeatureNotSupportedException  if the JDBC driver does not support this method
     * @since 1.4
     */
    override fun getGeneratedKeys(): ResultSet {
        throw SQLFeatureNotSupportedException("Method not supported")
    }

    /**
     * Retrieves the result set holdability for `ResultSet` objects
     * generated by this `Statement` object.
     *
     * @return either `ResultSet.HOLD_CURSORS_OVER_COMMIT` or
     * `ResultSet.CLOSE_CURSORS_AT_COMMIT`
     * @exception SQLException if a database access error occurs or
     * this method is called on a closed `Statement`
     *
     * @since 1.4
     */
    override fun getResultSetHoldability(): Int {
        throw SQLFeatureNotSupportedException("Method not supported")
    }

    /**
     * Retrieves whether this `Statement` object has been closed. A `Statement` is closed if the
     * method close has been called on it, or if it is automatically closed.
     * @return true if this `Statement` object is closed; false if it is still open
     * @throws SQLException if a database access error occurs
     * @since 1.6
     */
    override fun isClosed(): Boolean {
        TODO("Not yet implemented")
    }

    /**
     * Requests that a `Statement` be pooled or not pooled.  The value
     * specified is a hint to the statement pool implementation indicating
     * whether the application wants the statement to be pooled.  It is up to
     * the statement pool manager as to whether the hint is used.
     *
     *
     * The poolable value of a statement is applicable to both internal
     * statement caches implemented by the driver and external statement caches
     * implemented by application servers and other applications.
     *
     *
     * By default, a `Statement` is not poolable when created, and
     * a `PreparedStatement` and `CallableStatement`
     * are poolable when created.
     *
     *
     * @param poolable              requests that the statement be pooled if true and
     * that the statement not be pooled if false
     *
     *
     * @throws SQLException if this method is called on a closed
     * `Statement`
     *
     *
     * @since 1.6
     */
    override fun setPoolable(poolable: Boolean) {
        throw SQLFeatureNotSupportedException("Method not supported")
    }

    /**
     * Returns a  value indicating whether the `Statement`
     * is poolable or not.
     *
     *
     * @return              `true` if the `Statement`
     * is poolable; `false` otherwise
     *
     *
     * @throws SQLException if this method is called on a closed
     * `Statement`
     *
     *
     * @since 1.6
     *
     *
     * @see java.sql.Statement.setPoolable
     */
    override fun isPoolable(): Boolean {
        throw SQLFeatureNotSupportedException("Method not supported")
    }

    /**
     * Specifies that this `Statement` will be closed when all its
     * dependent result sets are closed. If execution of the `Statement`
     * does not produce any result sets, this method has no effect.
     *
     *
     * **Note:** Multiple calls to `closeOnCompletion` do
     * not toggle the effect on this `Statement`. However, a call to
     * `closeOnCompletion` does effect both the subsequent execution of
     * statements, and statements that currently have open, dependent,
     * result sets.
     *
     * @throws SQLException if this method is called on a closed
     * `Statement`
     * @since 1.7
     */
    override fun closeOnCompletion() {
        throw SQLFeatureNotSupportedException("Method not supported")
    }

    /**
     * Returns a value indicating whether this `Statement` will be
     * closed when all its dependent result sets are closed.
     * @return `true` if the `Statement` will be closed when all
     * of its dependent result sets are closed; `false` otherwise
     * @throws SQLException if this method is called on a closed
     * `Statement`
     * @since 1.7
     */
    override fun isCloseOnCompletion(): Boolean {
        throw SQLFeatureNotSupportedException("Method not supported")
    }
}
