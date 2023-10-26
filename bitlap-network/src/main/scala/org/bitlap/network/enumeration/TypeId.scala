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
package org.bitlap.network.enumeration

import java.sql.Types

import org.bitlap.network.Driver.*

import enumeratum.values.*

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

  def toTypeId(typeId: BTypeId): TypeId =
    TypeId.withValueOpt(typeId.value).getOrElse(Unspecified)

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
