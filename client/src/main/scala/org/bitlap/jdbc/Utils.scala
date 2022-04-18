/* Copyright (c) 2022 bitlap.org */
package org.bitlap.jdbc

import org.bitlap.jdbc.ColumnType.{ BOOLEAN, BYTE, DOUBLE, INT, LONG, STRING, TIMESTAMP }
import org.bitlap.network.proto.driver.BTypeId

/**
 * @author 梦境迷离
 * @since 2021/8/28
 * @version 1.0
 */
object Utils {

  /**
   * Major version number of this driver.
   */
  val MAJOR_VERSION = 0

  /**
   * Minor version number of this driver.
   */
  val MINOR_VERSION = 0

  /**
   * Is this driver JDBC compliant?
   */
  val JDBC_COMPLIANT = false

  /**
   * The required prefix for the connection url
   */
  val URL_PREFIX = "jdbc:bitlap://"

  /**
   * If host is provided, without a port
   */
  val DEFAULT_PORT = "10000"

  /**
   * Property key for the database name
   */
  val DBNAME_PROPERTY_KEY = "DBNAME"

  /**
   * Property key for the bitlap Server host
   */
  val HOST_PROPERTY_KEY = "HOST"

  /**
   * Property key for the bitlap Server port
   */
  val PORT_PROPERTY_KEY = "PORT"

  val SERVER_TYPE_NAMES =
    Map(
      BTypeId.B_TYPE_ID_STRING_TYPE -> ColumnType.STRING,
      BTypeId.B_TYPE_ID_INT_TYPE -> ColumnType.INT,
      BTypeId.B_TYPE_ID_DOUBLE_TYPE -> ColumnType.DOUBLE,
      BTypeId.B_TYPE_ID_SHORT_TYPE -> ColumnType.SHORT,
      BTypeId.B_TYPE_ID_BOOLEAN_TYPE -> ColumnType.BOOLEAN,
      BTypeId.B_TYPE_ID_LONG_TYPE -> ColumnType.LONG,
      BTypeId.B_TYPE_ID_TIMESTAMP_TYPE -> ColumnType.TIMESTAMP,
      BTypeId.B_TYPE_ID_BYTE_TYPE -> ColumnType.BYTE
    )
}
