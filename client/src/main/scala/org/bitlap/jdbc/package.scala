/* Copyright (c) 2022 bitlap.org */
package org.bitlap

import java.sql.Types

/**
 * @author 梦境迷离
 * @version 1.0,2022/4/18
 */
package object jdbc {

  sealed trait ColumnType {
    self: ColumnType =>

    import org.bitlap.jdbc.ColumnType._

    @inline final def stringify: String = self match {
      case STRING    => "STRING"
      case BOOLEAN   => "BOOLEAN"
      case DOUBLE    => "DOUBLE"
      case BYTE      => "BYTE"
      case TIMESTAMP => "TIMESTAMP"
      case INT       => "INT"
      case LONG      => "LONG"
      case SHORT     => "SHORT"
      case _         => throw BSQLException(s"Unrecognized column type: $self")
    }

    @inline final def toType: Int = self match {
      case STRING    => Types.VARCHAR
      case BOOLEAN   => Types.BOOLEAN
      case DOUBLE    => Types.DOUBLE
      case BYTE      => Types.TINYINT
      case TIMESTAMP => Types.TIMESTAMP
      case INT       => Types.INTEGER
      case LONG      => Types.BIGINT
      case SHORT     => Types.SMALLINT
      case _         => throw BSQLException(s"Unrecognized column type: $self")
    }

  }

  private[jdbc] object ColumnType {
    case object STRING extends ColumnType
    case object BOOLEAN extends ColumnType
    case object DOUBLE extends ColumnType
    case object BYTE extends ColumnType
    case object TIMESTAMP extends ColumnType
    case object INT extends ColumnType
    case object LONG extends ColumnType
    case object SHORT extends ColumnType
  }

  import org.bitlap.jdbc.ColumnType._

  // not reflect
  private final val types: List[ColumnType] = List(STRING, BOOLEAN, DOUBLE, BYTE, TIMESTAMP, INT, LONG, SHORT)

  case class ColumnTyped(typ: String) extends ColumnType {
    def toValue: Int =
      types
        .find(p => typ == p.stringify)
        .map(_.toType)
        .getOrElse(throw BSQLException(s"Unrecognized column type value: $typ"))
  }

}
