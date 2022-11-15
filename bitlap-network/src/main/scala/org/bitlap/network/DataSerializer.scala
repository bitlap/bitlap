/* Copyright (c) 2022 bitlap.org */
package org.bitlap.network

import org.bitlap.network.models.TypeId
import org.bitlap.network.NetworkException.DataFormatException

import java.io._
import java.nio.ByteBuffer
import java.sql._
import scala.annotation.implicitNotFound
import scala.reflect._
import scala.util.Using
import com.google.protobuf.ByteString

/** @author
 *    梦境迷离
 *  @version 1.0,2022/11/15
 */
trait DataSerializer {

  private final val TRUE = 1

  @implicitNotFound("Could not find an implicit ClassTag[\\${T}]")
  def deserialize[T: ClassTag](realType: TypeId, byteArray: scala.Array[Byte]): T =
    deserialize[T](realType, ByteString.copyFrom(ByteBuffer.wrap(byteArray)))

  @implicitNotFound("Could not find an implicit ClassTag[\\${T}]")
  def deserialize[T: ClassTag](realType: TypeId, byteString: ByteString): T = {
    val readOnlyByteBuffer = byteString.asReadOnlyByteBuffer()
    val typeName           = classTag[T].runtimeClass.getSimpleName
    val targetType = TypeId.values.find(_.name.toLowerCase == typeName.toLowerCase).getOrElse(TypeId.Unspecified)
    try {
      val r = realType match {
        case TypeId.IntType =>
          targetType match {
            case TypeId.BooleanType => readOnlyByteBuffer.getInt == TRUE
            case TypeId.StringType  => readOnlyByteBuffer.getInt.toString
            case TypeId.LongType    => readOnlyByteBuffer.getInt.toLong
            case TypeId.DoubleType  => readOnlyByteBuffer.getInt.toDouble
            case TypeId.FloatType   => readOnlyByteBuffer.getInt.toFloat
            case _                  => readOnlyByteBuffer.getInt
          }
        case TypeId.ByteType =>
          targetType match {
            case TypeId.StringType => readOnlyByteBuffer.get().toString
            case TypeId.IntType    => readOnlyByteBuffer.get().toInt
            case TypeId.ShortType  => readOnlyByteBuffer.get().toShort
            case TypeId.LongType   => readOnlyByteBuffer.get().toLong
            case _                 => readOnlyByteBuffer.get()
          }

        case TypeId.DoubleType =>
          targetType match {
            case TypeId.StringType => readOnlyByteBuffer.getDouble.toString
            case _                 => readOnlyByteBuffer.getDouble
          }

        case TypeId.ShortType =>
          targetType match {
            case TypeId.StringType => readOnlyByteBuffer.getShort.toString
            case TypeId.IntType    => readOnlyByteBuffer.getShort.toInt
            case TypeId.LongType   => readOnlyByteBuffer.getShort.toLong
            case TypeId.FloatType  => readOnlyByteBuffer.getShort.toFloat
            case TypeId.DoubleType => readOnlyByteBuffer.getShort.toDouble
            case _                 => readOnlyByteBuffer.getShort
          }

        case TypeId.LongType =>
          targetType match {
            case TypeId.StringType => readOnlyByteBuffer.getLong.toString
            case TypeId.DoubleType => readOnlyByteBuffer.getLong.toDouble
            case _                 => readOnlyByteBuffer.getLong
          }

        case TypeId.BooleanType =>
          targetType match {
            case TypeId.StringType => (readOnlyByteBuffer.get() == TRUE).toString
            case TypeId.IntType    => readOnlyByteBuffer.get().toInt
            case TypeId.ShortType  => readOnlyByteBuffer.get().toShort
            case TypeId.ByteType   => readOnlyByteBuffer.get()
            case _                 => readOnlyByteBuffer.get() == TRUE
          }
        case TypeId.FloatType =>
          targetType match {
            case TypeId.StringType => readOnlyByteBuffer.getFloat.toString
            case TypeId.DoubleType => readOnlyByteBuffer.getFloat.toDouble
            case _                 => readOnlyByteBuffer.getFloat
          }
        case TypeId.TimeType =>
          targetType match {
            case TypeId.StringType => new Time(readOnlyByteBuffer.getLong).toString
            case _                 => new Time(readOnlyByteBuffer.getLong)
          }
        case TypeId.DateType =>
          targetType match {
            case TypeId.StringType => new Date(readOnlyByteBuffer.getLong).toString
            case _                 => new Date(readOnlyByteBuffer.getLong)
          }
        case TypeId.TimestampType =>
          targetType match {
            case TypeId.StringType => new Timestamp(readOnlyByteBuffer.getLong).toString
            case _                 => new Timestamp(readOnlyByteBuffer.getLong)
          }
        case TypeId.StringType =>
          new String(byteString.toByteArray)
        case TypeId.Unspecified =>
          throw DataFormatException(msg = s"Incompatible type for realType:$realType, targetType:$targetType")
      }

      r.asInstanceOf[T]

    } catch {
      case e: Exception =>
        e.printStackTrace()
        null.asInstanceOf[T]
    }
  }

  def serialize(any: Any): ByteString = {
    val buffer = new ByteArrayOutputStream()
    Using.resources(buffer, new DataOutputStream(buffer)) { (_, d) =>
      any match {
        case i: Boolean             => d.writeBoolean(i)
        case i: Short               => d.writeShort(i)
        case i: Int                 => d.writeInt(i)
        case i: Long                => d.writeLong(i)
        case i: Float               => d.writeFloat(i)
        case i: Double              => d.writeDouble(i)
        case i: Char                => d.writeChar(i)
        case i: Byte                => d.writeByte(i)
        case i: java.lang.Boolean   => d.writeBoolean(i)
        case i: java.lang.Short     => d.writeShort(i.toInt)
        case i: java.lang.Integer   => d.writeInt(i)
        case i: java.lang.Long      => d.writeLong(i)
        case i: java.lang.Float     => d.writeFloat(i)
        case i: java.lang.Double    => d.writeDouble(i)
        case i: java.lang.Character => d.writeChar(i.toInt)
        case i: java.lang.Byte      => d.writeByte(i.toInt)
        case i: Timestamp           => d.writeLong(i.getTime)
        case i: Time                => d.writeLong(i.getTime)
        case i: Date                => d.writeLong(i.getTime)
        case i: String              => d.writeChars(i)
        case i                      => throw DataFormatException(msg = s"Unsupported type:$i")
      }
      d.flush()
    }
    ByteString.copyFrom(buffer.toByteArray)
  }
}
