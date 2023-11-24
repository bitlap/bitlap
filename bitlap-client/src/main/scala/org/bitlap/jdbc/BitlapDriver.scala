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

import java.sql.{ Array as _, * }
import java.util.Properties
import java.util.logging.Logger

import org.bitlap.common.exception.BitlapSQLException

/** Bitlap jdbc driver
 */
abstract class BitlapDriver extends Driver:

  override def connect(url: String, info: Properties): Connection =
    try
      if acceptsURL(url) then new BitlapConnection(url, info)
      else throw BitlapSQLException(s"Invalid bitlap jdbc url: $url")
    catch
      case ex: BitlapSQLException => throw ex
      case ex: Exception          => throw BitlapSQLException(ex.toString)

  /** Checks whether a given url is in a valid format.
   *
   *  The current uri format is: `jdbc:bitlap://[host[:port]]`
   *
   *  jdbc:bitlap:// - run in embedded mode jdbc:bitlap://localhost - connect to localhost default port (10000)
   *  jdbc:bitlap://localhost:5050 - connect to localhost port 5050
   *
   *  TODO - write a better regex.
   *    - decide on uri format
   */
  override def acceptsURL(url: String): Boolean =
    if url == null || url.isEmpty then false
    else url.startsWith(Constants.URL_PREFIX)

  override def getPropertyInfo(url: String, info: Properties): Array[DriverPropertyInfo] =
    var curInfo: Properties = new Properties(info)
    if url != null && url.startsWith(Constants.URL_PREFIX) then curInfo = parseURL(url, curInfo)

    val hostProp = new DriverPropertyInfo(
      Constants.HOST_PROPERTY_KEY,
      curInfo.getProperty(Constants.HOST_PROPERTY_KEY, "")
    )
    hostProp.required = false
    hostProp.description = "Hostname of Bitlap Server"

    val portProp = new DriverPropertyInfo(
      Constants.PORT_PROPERTY_KEY,
      curInfo.getProperty(Constants.PORT_PROPERTY_KEY, "")
    )
    portProp.required = false
    portProp.description = "Port number of Bitlap Server"

    val dbProp = new DriverPropertyInfo(
      Constants.DBNAME_PROPERTY_KEY,
      curInfo.getProperty(Constants.DBNAME_PROPERTY_KEY, Constants.DEFAULT_DB)
    )
    dbProp.required = false
    dbProp.description = "Database name"

    Array(hostProp, portProp, dbProp)

  override def getMajorVersion(): Int = Constants.MAJOR_VERSION

  override def getMinorVersion(): Int = Constants.MINOR_VERSION

  override def jdbcCompliant(): Boolean = Constants.JDBC_COMPLIANT

  override def getParentLogger(): Logger = Logger.getLogger("BitlapDriver")

  def register(): Unit =
    try java.sql.DriverManager.registerDriver(this)
    catch
      case e: Exception =>
        throw BitlapSQLException("Error occurred while registering JDBC driver", cause = Option(e))

  /** Takes a url in the form of jdbc:bitlap://[hostname1,hostname2]:[port]/[db_name] and parses it.
   *
   *  @param url
   *  @param defaults
   *  @return
   */
  private def parseURL(url: String, defaults: Properties): Properties =
    val urlProps = if defaults != null then new Properties(defaults) else new Properties()
    if !url.startsWith(Constants.URL_PREFIX) then throw BitlapSQLException(s"Invalid connection url: $url")
    if url.length <= Constants.URL_PREFIX.length then return urlProps

    // [hostname]:[port]/[db_name]
    val connectionInfo: String = url.substring(Constants.URL_PREFIX.length)

    // [hostname]:[port]/[db_name]
    val hostPortAndDatabase = connectionInfo.split("/", 2)

    // [hostname]:[port]
    if hostPortAndDatabase(0).nonEmpty then
      val hostAndPort = hostPortAndDatabase(0).split(":", 2)
      urlProps.setProperty(Constants.HOST_PROPERTY_KEY, hostAndPort(0))
      if hostAndPort.size > 1 then urlProps.setProperty(Constants.PORT_PROPERTY_KEY, hostAndPort(1))
      else urlProps.setProperty(Constants.PORT_PROPERTY_KEY, Constants.DEFAULT_PORT)

    // [db_name]
    if hostPortAndDatabase.size > 1 then urlProps.setProperty(Constants.DBNAME_PROPERTY_KEY, hostPortAndDatabase(1))
    urlProps
