/* Copyright (c) 2023 bitlap.org */
package org.bitlap.network.serde

import com.google.protobuf.ByteString
import org.bitlap.network.NetworkException.*
import org.bitlap.network.enumeration.TypeId
import org.bitlap.network.serde.BitlapDeserializer.parser

import java.io.*
import java.nio.ByteBuffer
import java.nio.charset.Charset
import java.sql.*
import scala.annotation.implicitNotFound
import scala.reflect.*
import scala.util.Using
import izumi.reflect.dottyreflection.*

/** @author
 *    梦境迷离
 *  @version 1.0,2022/11/15
 */
trait BitlapSerde:

  @implicitNotFound("Could not find an implicit ClassTag[\\${T}]")
  def deserialize[T: ClassTag](realType: TypeId, byteArray: scala.Array[Byte]): T =
    deserialize[T](realType, ByteString.copyFrom(ByteBuffer.wrap(byteArray)))

  @implicitNotFound("Could not find an implicit ClassTag[\\${T}]")
  def deserialize[T: ClassTag](realType: TypeId, byteString: ByteString): T =
    val readOnlyByteBuffer = byteString.asReadOnlyByteBuffer()
    val typeName           = classTag[T].runtimeClass.getSimpleName
    val targetType = TypeId.values.find(_.name.toLowerCase == typeName.toLowerCase).getOrElse(TypeId.Unspecified)
    try
      val r = realType match
        case TypeId.StringType =>
          new String(byteString.toByteArray, Charset.forName("utf8"))
        case TypeId.Unspecified =>
          throw DataFormatException(msg = s"Incompatible type for realType:$realType, targetType:$targetType")
        case _ => parser(realType).parse[T](readOnlyByteBuffer, targetType, realType)

      r.asInstanceOf[T]

    catch
      case e: Exception =>
        e.printStackTrace()
        null.asInstanceOf[T]

  def serialize(any: Any): ByteString =
    val buffer = new ByteArrayOutputStream()
    Using.resources(buffer, new DataOutputStream(buffer)) { (_, d) =>
      any match
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
        case i: String =>
          val chs = i.getBytes(Charset.forName("utf8"))
          d.write(chs)
        case i => throw DataFormatException(msg = s"Unsupported data:$i")
      d.flush()
    }
    ByteString.copyFrom(buffer.toByteArray)
