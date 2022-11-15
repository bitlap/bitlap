/* Copyright (c) 2022 bitlap.org */
package org.bitlap.network.test

import org.bitlap.network.DataSerializer
import org.bitlap.network.models.TypeId
import org.junit.Test
import java.sql.Date

import java.io._

/** @author
 *    梦境迷离
 *  @version 1.0,2022/11/15
 */
class DataSerializerSpec extends DataSerializer {

  @Test
  def testSerializeJavaInt(): Unit = {
    val s = serialize(Integer.valueOf(100))

    val buffer           = new ByteArrayOutputStream()
    val dataOutputStream = new DataOutputStream(buffer)
    dataOutputStream.writeInt(Integer.valueOf(100))
    dataOutputStream.flush()

    val d1 = deserialize[Int](TypeId.IntType, buffer.toByteArray)

    assert(s.asReadOnlyByteBuffer().getInt == d1)
  }

  @Test
  def testSerializeInt(): Unit = {
    val s = serialize(100)

    val buffer           = new ByteArrayOutputStream()
    val dataOutputStream = new DataOutputStream(buffer)
    dataOutputStream.writeInt(100)
    dataOutputStream.flush()

    val d1 = deserialize[Int](TypeId.IntType, buffer.toByteArray)

    assert(s.asReadOnlyByteBuffer().getInt == d1)
  }

  @Test
  def testSerializeLong(): Unit = {
    val s = serialize(100L)

    val buffer           = new ByteArrayOutputStream()
    val dataOutputStream = new DataOutputStream(buffer)
    dataOutputStream.writeLong(100L)
    dataOutputStream.flush()

    val d1 = deserialize[Long](TypeId.LongType, buffer.toByteArray)

    assert(s.asReadOnlyByteBuffer().getLong == d1)
  }

  @Test
  def testSerializeShort(): Unit = {
    val s                = serialize(100.toShort)
    val buffer           = new ByteArrayOutputStream()
    val dataOutputStream = new DataOutputStream(buffer)
    dataOutputStream.writeShort(100)
    dataOutputStream.flush()

    val d1 = deserialize[Short](TypeId.ShortType, buffer.toByteArray)

    assert(s.asReadOnlyByteBuffer().getShort() == d1)
  }

  @Test
  def testSerializeDouble(): Unit = {
    val s                = serialize(100d)
    val buffer           = new ByteArrayOutputStream()
    val dataOutputStream = new DataOutputStream(buffer)
    dataOutputStream.writeDouble(100d)
    dataOutputStream.flush()

    val d1 = deserialize[Double](TypeId.DoubleType, buffer.toByteArray)

    assert(s.asReadOnlyByteBuffer().getDouble == d1)
  }

  @Test
  def testSerializeFloat(): Unit = {
    val s                = serialize(100f)
    val buffer           = new ByteArrayOutputStream()
    val dataOutputStream = new DataOutputStream(buffer)
    dataOutputStream.writeFloat(100f)
    dataOutputStream.flush()

    val d1 = deserialize[Float](TypeId.FloatType, buffer.toByteArray)

    assert(s.asReadOnlyByteBuffer().getFloat == d1)
  }

  @Test
  def testSerializeString(): Unit = {
    val s                = serialize("10")
    val buffer           = new ByteArrayOutputStream()
    val dataOutputStream = new DataOutputStream(buffer)
    dataOutputStream.writeChars("10")
    dataOutputStream.flush()

    val d1 = deserialize[String](TypeId.StringType, buffer.toByteArray)

    assert(s"${new String(s.toByteArray)}" == d1)
  }

  @Test
  def testSerializeIntCanBeString(): Unit = {
    val s = serialize(100)

    val buffer           = new ByteArrayOutputStream()
    val dataOutputStream = new DataOutputStream(buffer)
    dataOutputStream.writeInt(100)
    dataOutputStream.flush()

    val d1 = deserialize[String](TypeId.IntType, buffer.toByteArray)

    assert(s.asReadOnlyByteBuffer().getInt == d1.toInt)
  }

  @Test
  def testSerializeLongCanBeDate(): Unit = {
    val date = new Date(System.currentTimeMillis())
    val s    = serialize(date)

    val buffer           = new ByteArrayOutputStream()
    val dataOutputStream = new DataOutputStream(buffer)
    dataOutputStream.writeLong(date.getTime)
    dataOutputStream.flush()

    val d1 = deserialize[Date](TypeId.DateType, buffer.toByteArray)

    assert(s.asReadOnlyByteBuffer().getLong == d1.getTime)
  }

  @Test
  def testSerializeBooleanCanBeInt(): Unit = {
    val s                = serialize(true)
    val buffer           = new ByteArrayOutputStream()
    val dataOutputStream = new DataOutputStream(buffer)
    dataOutputStream.writeBoolean(true)
    dataOutputStream.flush()

    val d1 = deserialize[Boolean](TypeId.BooleanType, buffer.toByteArray)

    assert(s.asReadOnlyByteBuffer().get() == 1 && d1)
  }

  @Test
  def testSerializeBooleanCanBeString(): Unit = {
    val s                = serialize(true)
    val buffer           = new ByteArrayOutputStream()
    val dataOutputStream = new DataOutputStream(buffer)
    dataOutputStream.writeBoolean(true)
    dataOutputStream.flush()

    val d1 = deserialize[String](TypeId.BooleanType, buffer.toByteArray)

    assert(s.asReadOnlyByteBuffer().get() == 1 && d1 == "true")
  }

  @Test
  def testSerializeShortCanBeString(): Unit = {
    val s                = serialize(100.toShort)
    val buffer           = new ByteArrayOutputStream()
    val dataOutputStream = new DataOutputStream(buffer)
    dataOutputStream.writeShort(100)
    dataOutputStream.flush()

    val d1 = deserialize[String](TypeId.ShortType, buffer.toByteArray)

    assert(s.asReadOnlyByteBuffer().getShort() == d1.toShort)
  }

}
