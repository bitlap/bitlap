/* Copyright (c) 2023 bitlap.org */
package org.bitlap.spark.jdbc

import org.apache.spark.sql.jdbc._
import org.apache.spark.sql.types._

object BitlapJdbcDialect extends JdbcDialect {

  override def canHandle(url: String): Boolean = url.startsWith("jdbc:bitlap")

  /** This is only called for ArrayType (see JdbcUtils.makeSetter)
   */
  override def getJDBCType(dt: DataType): Option[JdbcType] = dt match {
    case StringType => Some(JdbcType("VARCHAR", java.sql.Types.VARCHAR))
    case BinaryType => Some(JdbcType("BINARY(" + dt.defaultSize + ")", java.sql.Types.BINARY))
    case ByteType   => Some(JdbcType("TINYINT", java.sql.Types.TINYINT))
    case ShortType  => Some(JdbcType("SMALLINT", java.sql.Types.SMALLINT))
    case _          => None
  }

}
