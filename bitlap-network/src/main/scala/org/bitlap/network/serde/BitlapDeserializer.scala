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
package org.bitlap.network.serde

import java.nio.ByteBuffer
import java.sql.*

import org.bitlap.common.exception.DataFormatException
import org.bitlap.network.enumeration.TypeId

/** Byte array deserialization, compatible with different types as much as possible.
 */
private[network] sealed trait BitlapDeserializer:
  def parse[T](readOnlyByteBuffer: ByteBuffer, targetType: TypeId, realType: TypeId): T

private[network] object BitlapDeserializer:

  private final val TRUE = 1

  private[network] def parser(typeId: TypeId) = typeId match
    // Handling exceptions and zero values
    case TypeId.IntType       => intParser
    case TypeId.LongType      => longParser
    case TypeId.ShortType     => shortParser
    case TypeId.DoubleType    => doubleParser
    case TypeId.FloatType     => floatParser
    case TypeId.TimeType      => timeParser
    case TypeId.TimestampType => timestampParser
    case TypeId.DateType      => dateParser
    case TypeId.ByteType      => byteParser
    case TypeId.BooleanType   => booleanParser
    case _                    => throw DataFormatException(s"Invalid type for typeId:$typeId")

  private case object intParser extends BitlapDeserializer:

    override def parse[T](readOnlyByteBuffer: ByteBuffer, targetType: TypeId, realType: TypeId): T =
      val r = targetType match
        case TypeId.BooleanType => readOnlyByteBuffer.getInt == TRUE
        case TypeId.StringType  => readOnlyByteBuffer.getInt.toString
        case TypeId.LongType    => readOnlyByteBuffer.getInt.toLong
        case TypeId.DoubleType  => readOnlyByteBuffer.getInt.toDouble
        case TypeId.FloatType   => readOnlyByteBuffer.getInt.toFloat
        case _                  => readOnlyByteBuffer.getInt
      r.asInstanceOf[T]

  private case object shortParser extends BitlapDeserializer:

    override def parse[T](readOnlyByteBuffer: ByteBuffer, targetType: TypeId, realType: TypeId): T =
      val r = targetType match
        case TypeId.StringType => readOnlyByteBuffer.getShort.toString
        case TypeId.IntType    => readOnlyByteBuffer.getShort.toInt
        case TypeId.LongType   => readOnlyByteBuffer.getShort.toLong
        case TypeId.FloatType  => readOnlyByteBuffer.getShort.toFloat
        case TypeId.DoubleType => readOnlyByteBuffer.getShort.toDouble
        case _                 => readOnlyByteBuffer.getShort
      r.asInstanceOf[T]

  private case object doubleParser extends BitlapDeserializer:

    override def parse[T](readOnlyByteBuffer: ByteBuffer, targetType: TypeId, realType: TypeId): T =
      val r = targetType match
        case TypeId.StringType => readOnlyByteBuffer.getDouble.toString
        case _                 => readOnlyByteBuffer.getDouble
      r.asInstanceOf[T]

  private case object longParser extends BitlapDeserializer:

    override def parse[T](readOnlyByteBuffer: ByteBuffer, targetType: TypeId, realType: TypeId): T =
      val r = targetType match
        case TypeId.StringType => readOnlyByteBuffer.getLong.toString
        case TypeId.DoubleType => readOnlyByteBuffer.getLong.toDouble
        case _                 => readOnlyByteBuffer.getLong

      r.asInstanceOf[T]

  private case object booleanParser extends BitlapDeserializer:

    override def parse[T](readOnlyByteBuffer: ByteBuffer, targetType: TypeId, realType: TypeId): T =
      val r = targetType match
        case TypeId.StringType => (readOnlyByteBuffer.get() == TRUE).toString
        case TypeId.IntType    => readOnlyByteBuffer.get().toInt
        case TypeId.ShortType  => readOnlyByteBuffer.get().toShort
        case TypeId.ByteType   => readOnlyByteBuffer.get()
        case _                 => readOnlyByteBuffer.get() == TRUE

      r.asInstanceOf[T]

  private case object floatParser extends BitlapDeserializer:

    override def parse[T](readOnlyByteBuffer: ByteBuffer, targetType: TypeId, realType: TypeId): T =
      val r = targetType match
        case TypeId.StringType => readOnlyByteBuffer.getFloat.toString
        case TypeId.DoubleType => readOnlyByteBuffer.getFloat.toDouble
        case _                 => readOnlyByteBuffer.getFloat

      r.asInstanceOf[T]

  private case object timeParser extends BitlapDeserializer:

    override def parse[T](readOnlyByteBuffer: ByteBuffer, targetType: TypeId, realType: TypeId): T =
      val r = targetType match
        case TypeId.StringType => new Time(readOnlyByteBuffer.getLong).toString
        case TypeId.LongType   => readOnlyByteBuffer.getLong
        case _                 => new Time(readOnlyByteBuffer.getLong)

      r.asInstanceOf[T]

  private case object dateParser extends BitlapDeserializer:

    override def parse[T](readOnlyByteBuffer: ByteBuffer, targetType: TypeId, realType: TypeId): T =
      val r = targetType match
        case TypeId.StringType => new Date(readOnlyByteBuffer.getLong).toString
        case TypeId.LongType   => readOnlyByteBuffer.getLong
        case _                 => new Date(readOnlyByteBuffer.getLong)
      r.asInstanceOf[T]

  private case object timestampParser extends BitlapDeserializer:

    override def parse[T](readOnlyByteBuffer: ByteBuffer, targetType: TypeId, realType: TypeId): T =
      val r = targetType match
        case TypeId.StringType => new Timestamp(readOnlyByteBuffer.getLong).toString
        case TypeId.LongType   => readOnlyByteBuffer.getLong
        case _                 => new Timestamp(readOnlyByteBuffer.getLong)

      r.asInstanceOf[T]

  private case object byteParser extends BitlapDeserializer:

    override def parse[T](readOnlyByteBuffer: ByteBuffer, targetType: TypeId, realType: TypeId): T =
      val r = targetType match
        case TypeId.StringType => readOnlyByteBuffer.get().toString
        case TypeId.IntType    => readOnlyByteBuffer.get().toInt
        case TypeId.ShortType  => readOnlyByteBuffer.get().toShort
        case TypeId.LongType   => readOnlyByteBuffer.get().toLong
        case _                 => readOnlyByteBuffer.get()
      r.asInstanceOf[T]
