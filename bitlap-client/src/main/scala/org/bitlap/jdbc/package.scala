/* Copyright (c) 2022 bitlap.org */
package org.bitlap

import java.sql.Types

/** @author
 *    梦境迷离
 *  @version 1.0,2022/4/18
 */
package object jdbc {

  sealed trait ColumnType {
    self: ColumnType =>

    import org.bitlap.jdbc.ColumnType._

    @inline final def stringify: String = self match {
      case String    => "String"
      case Boolean   => "Boolean"
      case Double    => "Double"
      case Byte      => "Byte"
      case Timestamp => "Timestamp"
      case Int       => "Int"
      case Long      => "Long"
      case Short     => "Short"
      case _         => throw BSQLException(s"Unrecognized column type: $self")
    }

    @inline final def toType: Int = self match {
      case String    => Types.VARCHAR
      case Boolean   => Types.BOOLEAN
      case Double    => Types.DOUBLE
      case Byte      => Types.TINYINT
      case Timestamp => Types.TIMESTAMP
      case Int       => Types.INTEGER
      case Long      => Types.BIGINT
      case Short     => Types.SMALLINT
      case _         => throw BSQLException(s"Unrecognized column type: $self")
    }

  }

  private[jdbc] object ColumnType {
    case object String    extends ColumnType
    case object Boolean   extends ColumnType
    case object Double    extends ColumnType
    case object Byte      extends ColumnType
    case object Timestamp extends ColumnType
    case object Int       extends ColumnType
    case object Long      extends ColumnType
    case object Short     extends ColumnType
  }

  import org.bitlap.jdbc.ColumnType._

  // not reflect
  private final val types: List[ColumnType] = List(String, Boolean, Double, Byte, Timestamp, Int, Long, Short)

  final case class ColumnTyped(typ: String) extends ColumnType {
    def toValue: Int =
      types
        .find(p => typ == p.stringify)
        .map(_.toType)
        .getOrElse(throw BSQLException(s"Unrecognized column type value: $typ"))
  }

}
