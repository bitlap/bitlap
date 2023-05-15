/* Copyright (c) 2023 bitlap.org */
package org.bitlap.network.enumeration

import java.sql.Types

import org.bitlap.network.driver_proto.*

import enumeratum.values.*

/** @author
 *    梦境迷离
 *  @version 1.0,2023/3/18
 */
sealed abstract class TypeId(val value: Int, val name: String) extends IntEnumEntry

object TypeId extends IntEnum[TypeId]:
  case object Unspecified   extends TypeId(0, "Any")
  case object StringType    extends TypeId(1, "String")
  case object IntType       extends TypeId(2, "Int")
  case object DoubleType    extends TypeId(3, "Double")
  case object LongType      extends TypeId(4, "Long")
  case object BooleanType   extends TypeId(5, "Boolean")
  case object TimestampType extends TypeId(6, "Timestamp")
  case object ShortType     extends TypeId(7, "Short")
  case object ByteType      extends TypeId(8, "Byte")
  case object FloatType     extends TypeId(9, "Float")
  case object TimeType      extends TypeId(10, "Time")
  case object DateType      extends TypeId(11, "Date")

  val values: IndexedSeq[TypeId] = findValues

  def toTypeId(bTypeId: BTypeId): TypeId =
    TypeId.withValueOpt(bTypeId.value).getOrElse(Unspecified)

  def toBTypeId(typeId: TypeId): BTypeId =
    BTypeId.fromValue(typeId.value)

  def jdbc2Bitlap: Map[Int, TypeId] = Map(
    Types.VARCHAR   -> TypeId.StringType,
    Types.SMALLINT  -> TypeId.ShortType,
    Types.INTEGER   -> TypeId.IntType,
    Types.BIGINT    -> TypeId.LongType,
    Types.DOUBLE    -> TypeId.DoubleType,
    Types.BOOLEAN   -> TypeId.BooleanType,
    Types.TIMESTAMP -> TypeId.TimestampType,
    Types.TINYINT   -> TypeId.ByteType,
    Types.FLOAT     -> TypeId.FloatType,
    Types.TIME      -> TypeId.TimeType,
    Types.DATE      -> TypeId.DateType
  )

  def bitlap2Jdbc: Map[TypeId, Int] = jdbc2Bitlap.map(kv => kv._2 -> kv._1)
