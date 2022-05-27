/* Copyright (c) 2022 bitlap.org */
package org.bitlap.jdbc

import org.slf4j
import org.slf4j.LoggerFactory

import java.sql.{ Connection, Driver, DriverPropertyInfo, SQLException }
import java.util.Properties
import java.util.logging.Logger

/** Desc: JDBC driver for bitlap
 *
 *  Mail: chk19940609@gmail.com Created by IceMimosa Date: 2021/4/16
 */
private[jdbc] abstract class BitlapDriver extends Driver {

  override def connect(url: String, info: Properties): Connection =
    try BitlapConnection(url, info)
    catch {
      case ex: Exception => throw BSQLException(ex.toString)
    }

  /** Checks whether a given url is in a valid format.
   *
   *  The current uri format is: `jdbc:bitlap://[host[:port]]`
   *
   *  jdbc:bitlap:// - run in embedded mode jdbc:bitlap://localhost - connect to localhost default port (10000)
   *  jdbc:bitlap://localhost:5050 - connect to localhost port 5050
   *
   *  TODO: - write a better regex.
   *    - decide on uri format
   */
  override def acceptsURL(url: String): Boolean =
    if (url == null || url.isEmpty) false
    else url.startsWith(Utils.URL_PREFIX)

  override def getPropertyInfo(url: String, info: Properties): Array[DriverPropertyInfo] = {
    var curInfo: Properties = new Properties(info)
    if (url != null && url.startsWith(Utils.URL_PREFIX)) {
      curInfo = parseURL(url, curInfo)
    }

    val hostProp = new DriverPropertyInfo(
      Utils.HOST_PROPERTY_KEY,
      curInfo.getProperty(Utils.HOST_PROPERTY_KEY, "")
    )
    hostProp.required = false
    hostProp.description = "Hostname of Bitlap Server"

    val portProp = new DriverPropertyInfo(
      Utils.PORT_PROPERTY_KEY,
      curInfo.getProperty(Utils.PORT_PROPERTY_KEY, "")
    )
    portProp.required = false
    portProp.description = "Port number of Bitlap Server"

    val dbProp = new DriverPropertyInfo(
      Utils.DBNAME_PROPERTY_KEY,
      curInfo.getProperty(Utils.DBNAME_PROPERTY_KEY, "default")
    )
    dbProp.required = false
    dbProp.description = "Database name"

    println(s"Driver connect to: host[${hostProp.value}],port[${portProp.value}],database:[${dbProp.value}]")
    Array(hostProp, portProp, dbProp)
  }

  override def getMajorVersion(): Int = Utils.MAJOR_VERSION

  override def getMinorVersion(): Int = Utils.MINOR_VERSION

  override def jdbcCompliant(): Boolean = Utils.JDBC_COMPLIANT

  override def getParentLogger(): Logger = Logger.getLogger("BitlapDriver")

  def register(): Unit =
    try java.sql.DriverManager.registerDriver(this)
    catch {
      case e: Exception =>
        throw BSQLException("Error occurred while registering JDBC driver", cause = e)
    }

  /** Takes a url in the form of jdbc:bitlap://[hostname1,hostname2]:[port]/[db_name] and parses it.
   *
   *  @param url
   *  @param defaults
   *  @return
   */
  private def parseURL(url: String, defaults: Properties): Properties = {
    val urlProps = if (defaults != null) new Properties(defaults) else new Properties()
    if (!url.startsWith(Utils.URL_PREFIX)) {
      throw BSQLException(s"Invalid connection url: $url")
    }
    if (url.length <= Utils.URL_PREFIX.length) return urlProps

    // [hostname]:[port]/[db_name]
    val connectionInfo: String = url.substring(Utils.URL_PREFIX.length)

    // [hostname]:[port]/[db_name]
    val hostPortAndDatabase = connectionInfo.split("/", 2)

    // [hostname]:[port]
    if (hostPortAndDatabase(0).nonEmpty) {
      val hostAndPort = hostPortAndDatabase(0).split(":", 2)
      urlProps.setProperty(Utils.HOST_PROPERTY_KEY, hostAndPort(0))
      if (hostAndPort.size > 1) {
        urlProps.setProperty(Utils.PORT_PROPERTY_KEY, hostAndPort(1))
      } else {
        urlProps.setProperty(Utils.PORT_PROPERTY_KEY, Utils.DEFAULT_PORT)
      }
    }

    // [db_name]
    if (hostPortAndDatabase.size > 1) {
      urlProps.setProperty(Utils.DBNAME_PROPERTY_KEY, hostPortAndDatabase(1))
    }
    urlProps
  }
}
