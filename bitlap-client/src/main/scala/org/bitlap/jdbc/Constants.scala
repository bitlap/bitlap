/* Copyright (c) 2023 bitlap.org */
package org.bitlap.jdbc

/** bitlap 客户端的常量
 *  @author
 *    梦境迷离
 *  @since 2021/8/28
 *  @version 1.0
 */
object Constants {

  /** Major version number of this driver.
   */
  val MAJOR_VERSION = 0

  /** Minor version number of this driver.
   */
  val MINOR_VERSION = 0

  /** Is this driver JDBC compliant?
   */
  val JDBC_COMPLIANT = false

  /** The required prefix for the connection url
   */
  val URL_PREFIX = "jdbc:bitlap://"

  /** If host is provided, without a port
   */
  val DEFAULT_PORT = "23333"

  /** Property key for the database name
   */
  val DBNAME_PROPERTY_KEY = "DBNAME"

  /** Property key for the bitlap Server host
   */
  val HOST_PROPERTY_KEY = "HOST"

  /** Property key for the bitlap Server port
   */
  val PORT_PROPERTY_KEY = "PORT"

  val DEFAULT_DB = "default"
}
