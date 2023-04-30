/* Copyright (c) 2023 bitlap.org */
package org.bitlap.network.enumeration

import enumeratum.values.*
import org.bitlap.network.driver_proto.*

import java.sql.Types

/** @author
 *    梦境迷离
 *  @version 1.0,2023/3/18
 */
sealed abstract class TypeId(val value: Int, val name: String) extends IntEnumEntry

object TypeId extends IntEnum[TypeId]:
  final case object Unspecified   extends TypeId(0, "Any")
  final case object StringType    extends TypeId(1, "String")
  final case object IntType       extends TypeId(2, "Int")
  final case object DoubleType    extends TypeId(3, "Double")
  final case object LongType      extends TypeId(4, "Long")
  final case object BooleanType   extends TypeId(5, "Boolean")
  final case object TimestampType extends TypeId(6, "Timestamp")
  final case object ShortType     extends TypeId(7, "Short")
  final case object ByteType      extends TypeId(8, "Byte")
  final case object FloatType     extends TypeId(9, "Float")
  final case object TimeType      extends TypeId(10, "Time")
  final case object DateType      extends TypeId(11, "Date")

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
