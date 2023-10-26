/*
 * Copyright 2020-2023 IceMimosa, jxnu-liguobin and the Bitlap Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.bitlap.jdbc

import java.io.PrintWriter
import java.sql.{ Connection, SQLFeatureNotSupportedException }
import java.util.Properties
import java.util.logging.Logger
import javax.sql.DataSource

/** Bitlap JDBC datasource has not yet implemented authentication and connection parameter configuration.
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
