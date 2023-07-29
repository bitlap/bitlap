/**
 * Copyright (C) 2023 bitlap.org .
 */
package org.bitlap.network.test

import java.io.*
import java.sql.{ Date, Time, Timestamp }

import org.bitlap.network.enumeration.TypeId
import org.bitlap.network.serde.BitlapSerde

import org.junit.Test

/** @author
 *    梦境迷离
 *  @version 1.0,2022/11/15
 */
class DataSerializerSpec extends BitlapSerde:

  @Test
  def testSerializeJavaInt(): Unit =
    val s = serialize(Integer.valueOf(100))

    val buffer           = new ByteArrayOutputStream()
    val dataOutputStream = new DataOutputStream(buffer)
    dataOutputStream.writeInt(Integer.valueOf(100))
    dataOutputStream.flush()

    val d1 = deserialize[Int](TypeId.IntType, buffer.toByteArray)

    assert(s.asReadOnlyByteBuffer().getInt == d1)

  @Test
  def testSerializeInt(): Unit =
    val s = serialize(100)

    val buffer           = new ByteArrayOutputStream()
    val dataOutputStream = new DataOutputStream(buffer)
    dataOutputStream.writeInt(100)
    dataOutputStream.flush()

    val d1 = deserialize[Int](TypeId.IntType, buffer.toByteArray)

    assert(s.asReadOnlyByteBuffer().getInt == d1)

  @Test
  def testSerializeLong(): Unit =
    val s = serialize(100L)

    val buffer           = new ByteArrayOutputStream()
    val dataOutputStream = new DataOutputStream(buffer)
    dataOutputStream.writeLong(100L)
    dataOutputStream.flush()

    val d1 = deserialize[Long](TypeId.LongType, buffer.toByteArray)

    assert(s.asReadOnlyByteBuffer().getLong == d1)

  @Test
  def testSerializeShort(): Unit =
    val s                = serialize(100.toShort)
    val buffer           = new ByteArrayOutputStream()
    val dataOutputStream = new DataOutputStream(buffer)
    dataOutputStream.writeShort(100)
    dataOutputStream.flush()

    val d1 = deserialize[Short](TypeId.ShortType, buffer.toByteArray)

    assert(s.asReadOnlyByteBuffer().getShort() == d1)

  @Test
  def testSerializeDouble(): Unit =
    val s                = serialize(100d)
    val buffer           = new ByteArrayOutputStream()
    val dataOutputStream = new DataOutputStream(buffer)
    dataOutputStream.writeDouble(100d)
    dataOutputStream.flush()

    val d1 = deserialize[Double](TypeId.DoubleType, buffer.toByteArray)

    assert(s.asReadOnlyByteBuffer().getDouble == d1)

  @Test
  def testSerializeFloat(): Unit =
    val s                = serialize(100f)
    val buffer           = new ByteArrayOutputStream()
    val dataOutputStream = new DataOutputStream(buffer)
    dataOutputStream.writeFloat(100f)
    dataOutputStream.flush()

    val d1 = deserialize[Float](TypeId.FloatType, buffer.toByteArray)

    assert(s.asReadOnlyByteBuffer().getFloat == d1)

  @Test
  def testSerializeTimestamp(): Unit =
    val time             = new Timestamp(System.currentTimeMillis())
    val s                = serialize(time)
    val buffer           = new ByteArrayOutputStream()
    val dataOutputStream = new DataOutputStream(buffer)
    dataOutputStream.writeLong(time.getTime)
    dataOutputStream.flush()

    val d1 = deserialize[Timestamp](TypeId.TimestampType, buffer.toByteArray)

    assert(s.asReadOnlyByteBuffer().getLong == d1.getTime)

  @Test
  def testSerializeTime(): Unit =
    val time             = new Time(System.currentTimeMillis())
    val s                = serialize(time)
    val buffer           = new ByteArrayOutputStream()
    val dataOutputStream = new DataOutputStream(buffer)
    dataOutputStream.writeLong(time.getTime)
    dataOutputStream.flush()

    val d1 = deserialize[Time](TypeId.TimeType, buffer.toByteArray)

    assert(s.asReadOnlyByteBuffer().getLong == d1.getTime)

  @Test
  def testSerializeDate(): Unit =
    val time             = new Date(System.currentTimeMillis())
    val s                = serialize(time)
    val buffer           = new ByteArrayOutputStream()
    val dataOutputStream = new DataOutputStream(buffer)
    dataOutputStream.writeLong(time.getTime)
    dataOutputStream.flush()

    val d1 = deserialize[Date](TypeId.DateType, buffer.toByteArray)

    assert(s.asReadOnlyByteBuffer().getLong == d1.getTime)

  @Test
  def testSerializeDateCanBeLong(): Unit =
    val time             = new Date(System.currentTimeMillis())
    val s                = serialize(time)
    val buffer           = new ByteArrayOutputStream()
    val dataOutputStream = new DataOutputStream(buffer)
    dataOutputStream.writeLong(time.getTime)
    dataOutputStream.flush()

    val d1 = deserialize[Long](TypeId.DateType, buffer.toByteArray)

    assert(s.asReadOnlyByteBuffer().getLong == d1)

  @Test
  def testSerializeTimestampCanBeString(): Unit =
    val time             = new Timestamp(System.currentTimeMillis())
    val s                = serialize(time)
    val buffer           = new ByteArrayOutputStream()
    val dataOutputStream = new DataOutputStream(buffer)
    dataOutputStream.writeLong(time.getTime)
    dataOutputStream.flush()

    val d1 = deserialize[String](TypeId.TimestampType, buffer.toByteArray)

    assert(new Timestamp((s.asReadOnlyByteBuffer().getLong)).toString == d1)

  @Test
  def testSerializeString(): Unit =
    import java.nio.charset.Charset
    val s                = serialize("test_database_811656")
    val buffer           = new ByteArrayOutputStream()
    val dataOutputStream = new DataOutputStream(buffer)
    dataOutputStream.write("test_database_811656".getBytes(Charset.forName("utf8")))
    dataOutputStream.flush()

    val d1 = deserialize[String](TypeId.StringType, buffer.toByteArray)

    assert(s"${new String(s.toByteArray, Charset.forName("utf8"))}" == d1)

  @Test
  def testSerializeIntCanBeString(): Unit =
    val s = serialize(100)

    val buffer           = new ByteArrayOutputStream()
    val dataOutputStream = new DataOutputStream(buffer)
    dataOutputStream.writeInt(100)
    dataOutputStream.flush()

    val d1 = deserialize[String](TypeId.IntType, buffer.toByteArray)

    assert(s.asReadOnlyByteBuffer().getInt == d1.toInt)

  @Test
  def testSerializeLongCanBeDate(): Unit =
    val date = new Date(System.currentTimeMillis())
    val s    = serialize(date)

    val buffer           = new ByteArrayOutputStream()
    val dataOutputStream = new DataOutputStream(buffer)
    dataOutputStream.writeLong(date.getTime)
    dataOutputStream.flush()

    val d1 = deserialize[Date](TypeId.DateType, buffer.toByteArray)

    assert(s.asReadOnlyByteBuffer().getLong == d1.getTime)

  @Test
  def testSerializeBooleanCanBeInt(): Unit =
    val s                = serialize(true)
    val buffer           = new ByteArrayOutputStream()
    val dataOutputStream = new DataOutputStream(buffer)
    dataOutputStream.writeBoolean(true)
    dataOutputStream.flush()

    val d1 = deserialize[Boolean](TypeId.BooleanType, buffer.toByteArray)

    assert(s.asReadOnlyByteBuffer().get() == 1 && d1)

  @Test
  def testSerializeBooleanCanBeString(): Unit =
    val s                = serialize(true)
    val buffer           = new ByteArrayOutputStream()
    val dataOutputStream = new DataOutputStream(buffer)
    dataOutputStream.writeBoolean(true)
    dataOutputStream.flush()

    val d1 = deserialize[String](TypeId.BooleanType, buffer.toByteArray)

    assert(s.asReadOnlyByteBuffer().get() == 1 && d1 == "true")

  @Test
  def testSerializeShortCanBeString(): Unit =
    val s                = serialize(100.toShort)
    val buffer           = new ByteArrayOutputStream()
    val dataOutputStream = new DataOutputStream(buffer)
    dataOutputStream.writeShort(100)
    dataOutputStream.flush()

    val d1 = deserialize[String](TypeId.ShortType, buffer.toByteArray)

    assert(s.asReadOnlyByteBuffer().getShort() == d1.toShort)
