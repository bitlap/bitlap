/* Copyright (c) 2022 bitlap.org */
package org.bitlap.network

import java.io._
import java.sql._
import java.nio.ByteBuffer
import org.bitlap.network.models.TypeId

import scala.reflect.ClassTag
import scala.reflect.classTag
import scala.annotation.implicitNotFound
import scala.util.Using
import org.bitlap.network.NetworkException.DataFormatException

/** @author
 *    梦境迷离
 *  @version 1.0,2022/11/15
 */
trait DataSerializer {

  @implicitNotFound("Could not find an implicit ClassTag[\\${T}]")
  def deserialize[T: ClassTag](realType: TypeId, byteArray: scala.Array[Byte]): T =
    deserialize[T](realType, ByteBuffer.wrap(byteArray))

  @implicitNotFound("Could not find an implicit ClassTag[\\${T}]")
  def deserialize[T: ClassTag](realType: TypeId, byteBuffer: ByteBuffer): T = {
    val typeName   = classTag[T].runtimeClass.getSimpleName
    val targetType = TypeId.values.find(_.name.toLowerCase == typeName.toLowerCase).getOrElse(TypeId.Unspecified)
    try {
      val r = realType match {
        case TypeId.IntType =>
          targetType match {
            case TypeId.BooleanType => byteBuffer.getInt == 1
            case TypeId.StringType  => byteBuffer.getInt.toString
            case _                  => byteBuffer.getInt
          }
        case TypeId.DoubleType =>
          targetType match {
            case TypeId.StringType => byteBuffer.getDouble.toString
            case _                 => byteBuffer.getDouble
          }

        case TypeId.ShortType =>
          targetType match {
            case TypeId.StringType => byteBuffer.getShort.toString
            case TypeId.IntType    => byteBuffer.getShort.toInt
            case TypeId.LongType   => byteBuffer.getShort.toLong
            case TypeId.FloatType  => byteBuffer.getShort.toFloat
            case TypeId.DoubleType => byteBuffer.getShort.toDouble
            case _                 => byteBuffer.getShort
          }

        case TypeId.LongType =>
          targetType match {
            case TypeId.StringType => byteBuffer.getLong.toString
            case TypeId.IntType    => byteBuffer.getLong.toInt
            case TypeId.DoubleType => byteBuffer.getLong.toDouble
            case _                 => byteBuffer.getLong
          }

        case TypeId.BooleanType =>
          targetType match {
            case TypeId.StringType => (byteBuffer.get() == 1).toString
            case TypeId.IntType    => byteBuffer.get().toInt
            case _                 => byteBuffer.get() == 1
          }

        case TypeId.TimestampType =>
          targetType match {
            case TypeId.StringType => new Timestamp(byteBuffer.getLong).toString
            case _                 => new Timestamp(byteBuffer.getLong)
          }

        case TypeId.FloatType =>
          targetType match {
            case TypeId.StringType => byteBuffer.getFloat.toString
            case TypeId.DoubleType => byteBuffer.getFloat.toDouble
            case _                 => byteBuffer.getFloat
          }
        case TypeId.TimeType =>
          targetType match {
            case TypeId.StringType => new Time(byteBuffer.getLong).toString
            case _                 => new Time(byteBuffer.getLong)
          }
        case TypeId.DateType =>
          targetType match {
            case TypeId.StringType => new Date(byteBuffer.getLong).toString
            case _                 => new Date(byteBuffer.getLong)
          }
        case TypeId.StringType =>
          targetType match {
            case TypeId.IntType   => new String(byteBuffer.array()).toInt
            case TypeId.LongType  => new String(byteBuffer.array()).toLong
            case TypeId.ShortType => new String(byteBuffer.array()).toShort
            case _                => new String(byteBuffer.array())
          }
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

  def serialize(any: Any): ByteBuffer = {
    val buffer = new ByteArrayOutputStream()
    Using.resource(new DataOutputStream(buffer)) { d =>
      any match {
        case i: Boolean   => d.writeBoolean(i)
        case i: Short     => d.writeShort(i)
        case i: Int       => d.writeInt(i)
        case i: Long      => d.writeLong(i)
        case i: Float     => d.writeFloat(i)
        case i: Double    => d.writeDouble(i)
        case i: Char      => d.writeChar(i)
        case i: Byte      => d.writeByte(i)
        case i: Timestamp => d.writeLong(i.getTime)
        case i: Date      => d.writeLong(i.getTime)
        case i: String    => d.writeChars(i)
        case i            => throw DataFormatException(msg = s"Unsupported type:$i")
      }
      d.flush()
    }
    val r = ByteBuffer.wrap(buffer.toByteArray)
    buffer.close()
    r
  }
}
