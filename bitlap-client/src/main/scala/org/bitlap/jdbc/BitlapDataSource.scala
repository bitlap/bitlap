/**
 * Copyright (C) 2023 bitlap.org .
 */
package org.bitlap.jdbc

import java.io.PrintWriter
import java.sql.{ Connection, SQLFeatureNotSupportedException }
import java.util.Properties
import java.util.logging.Logger
import javax.sql.DataSource

/** Bitlap JDBC datasource has not yet implemented authentication and connection parameter configuration.
 *
 *  @author
 *    梦境迷离
 *  @since 2021/6/12
 *  @version 1.0
 */
class BitlapDataSource extends DataSource:

  override def unwrap[T](iface: Class[T]): T = throw new SQLFeatureNotSupportedException("Method not supported")

  override def isWrapperFor(iface: Class[?]): Boolean = throw new SQLFeatureNotSupportedException(
    "Method not supported"
  )

  override def getLogWriter: PrintWriter = throw new SQLFeatureNotSupportedException("Method not supported")

  override def setLogWriter(out: PrintWriter): Unit = throw new SQLFeatureNotSupportedException("Method not supported")

  override def setLoginTimeout(seconds: Int): Unit = throw new SQLFeatureNotSupportedException("Method not supported")

  override def getLoginTimeout: Int = throw new SQLFeatureNotSupportedException("Method not supported")

  override def getParentLogger: Logger = throw new SQLFeatureNotSupportedException("Method not supported")

  // TODO connect pool
  override def getConnection(): Connection = getConnection("", "")

  override def getConnection(username: String, password: String): Connection =
    try new BitlapConnection("", new Properties())
    catch case ex: Exception => throw BitlapSQLException(msg = "Error in getting BitlapConnection", cause = Option(ex))
