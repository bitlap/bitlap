/* Copyright (c) 2022 bitlap.org */
package org.bitlap.jdbc

import org.bitlap.network.models.TypeId

/** @author
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
  val DEFAULT_PORT = "10000"

  /** Property key for the database name
   */
  val DBNAME_PROPERTY_KEY = "DBNAME"

  /** Property key for the bitlap Server host
   */
  val HOST_PROPERTY_KEY = "HOST"

  /** Property key for the bitlap Server port
   */
  val PORT_PROPERTY_KEY = "PORT"

  val SERVER_TYPE_NAMES: Map[TypeId, ColumnType] =
    Map(
      TypeId.StringType    -> ColumnType.String,
      TypeId.IntType       -> ColumnType.Int,
      TypeId.DoubleType    -> ColumnType.Double,
      TypeId.ShortType     -> ColumnType.Short,
      TypeId.BooleanType   -> ColumnType.Boolean,
      TypeId.LongType      -> ColumnType.Long,
      TypeId.TimestampType -> ColumnType.Timestamp,
      TypeId.ByteType      -> ColumnType.Byte
    )
}
