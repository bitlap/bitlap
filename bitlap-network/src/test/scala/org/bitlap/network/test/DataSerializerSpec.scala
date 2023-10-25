/**
 * Copyright (C) 2023 bitlap.org .
 */
package org.bitlap.network.test

import java.io.*
import java.sql.{ Date, Time, Timestamp }

import org.bitlap.network.enumeration.TypeId
import org.bitlap.network.serde.BitlapSerde

import org.scalatest.funsuite.AnyFunSuite

/** @author
 *    梦境迷离
 *  @version 1.0,2022/11/15
 */
class DataSerializerSpec extends AnyFunSuite with BitlapSerde:

  test("testSerializeJavaInt") {
    val s = serialize(Integer.valueOf(100))

    val buffer           = new ByteArrayOutputStream()
    val dataOutputStream = new DataOutputStream(buffer)
    dataOutputStream.writeInt(Integer.valueOf(100))
    dataOutputStream.flush()

    val d1 = deserialize[Int](TypeId.IntType, buffer.toByteArray)

    assert(s.asReadOnlyByteBuffer().getInt == d1)
  }

  test("testSerializeInt") {

    val s = serialize(100)

    val buffer           = new ByteArrayOutputStream()
    val dataOutputStream = new DataOutputStream(buffer)
    dataOutputStream.writeInt(100)
    dataOutputStream.flush()

    val d1 = deserialize[Int](TypeId.IntType, buffer.toByteArray)

    assert(s.asReadOnlyByteBuffer().getInt == d1)
  }

  test("testSerializeLong") {

    val s = serialize(100L)

    val buffer           = new ByteArrayOutputStream()
    val dataOutputStream = new DataOutputStream(buffer)
    dataOutputStream.writeLong(100L)
    dataOutputStream.flush()

    val d1 = deserialize[Long](TypeId.LongType, buffer.toByteArray)

    assert(s.asReadOnlyByteBuffer().getLong == d1)
  }

  test("testSerializeShort") {

    val s                = serialize(100.toShort)
    val buffer           = new ByteArrayOutputStream()
    val dataOutputStream = new DataOutputStream(buffer)
    dataOutputStream.writeShort(100)
    dataOutputStream.flush()

    val d1 = deserialize[Short](TypeId.ShortType, buffer.toByteArray)

    assert(s.asReadOnlyByteBuffer().getShort() == d1)
  }

  test("testSerializeDouble") {
    val s                = serialize(100d)
    val buffer           = new ByteArrayOutputStream()
    val dataOutputStream = new DataOutputStream(buffer)
    dataOutputStream.writeDouble(100d)
    dataOutputStream.flush()

    val d1 = deserialize[Double](TypeId.DoubleType, buffer.toByteArray)

    assert(s.asReadOnlyByteBuffer().getDouble == d1)
  }

  test("testSerializeFloat") {
    val s                = serialize(100f)
    val buffer           = new ByteArrayOutputStream()
    val dataOutputStream = new DataOutputStream(buffer)
    dataOutputStream.writeFloat(100f)
    dataOutputStream.flush()

    val d1 = deserialize[Float](TypeId.FloatType, buffer.toByteArray)

    assert(s.asReadOnlyByteBuffer().getFloat == d1)
  }

  test("testSerializeTimestamp") {
    val time             = new Timestamp(System.currentTimeMillis())
    val s                = serialize(time)
    val buffer           = new ByteArrayOutputStream()
    val dataOutputStream = new DataOutputStream(buffer)
    dataOutputStream.writeLong(time.getTime)
    dataOutputStream.flush()

    val d1 = deserialize[Timestamp](TypeId.TimestampType, buffer.toByteArray)

    assert(s.asReadOnlyByteBuffer().getLong == d1.getTime)
  }

  test("testSerializeTime") {
    val time             = new Time(System.currentTimeMillis())
    val s                = serialize(time)
    val buffer           = new ByteArrayOutputStream()
    val dataOutputStream = new DataOutputStream(buffer)
    dataOutputStream.writeLong(time.getTime)
    dataOutputStream.flush()

    val d1 = deserialize[Time](TypeId.TimeType, buffer.toByteArray)

    assert(s.asReadOnlyByteBuffer().getLong == d1.getTime)
  }

  test("testSerializeDate") {
    val time             = new Date(System.currentTimeMillis())
    val s                = serialize(time)
    val buffer           = new ByteArrayOutputStream()
    val dataOutputStream = new DataOutputStream(buffer)
    dataOutputStream.writeLong(time.getTime)
    dataOutputStream.flush()

    val d1 = deserialize[Date](TypeId.DateType, buffer.toByteArray)

    assert(s.asReadOnlyByteBuffer().getLong == d1.getTime)
  }

  test("testSerializeDateCanBeLong") {
    val time             = new Date(System.currentTimeMillis())
    val s                = serialize(time)
    val buffer           = new ByteArrayOutputStream()
    val dataOutputStream = new DataOutputStream(buffer)
    dataOutputStream.writeLong(time.getTime)
    dataOutputStream.flush()

    val d1 = deserialize[Long](TypeId.DateType, buffer.toByteArray)

    assert(s.asReadOnlyByteBuffer().getLong == d1)
  }

  test("testSerializeTimestampCanBeString") {
    val time             = new Timestamp(System.currentTimeMillis())
    val s                = serialize(time)
    val buffer           = new ByteArrayOutputStream()
    val dataOutputStream = new DataOutputStream(buffer)
    dataOutputStream.writeLong(time.getTime)
    dataOutputStream.flush()

    val d1 = deserialize[String](TypeId.TimestampType, buffer.toByteArray)

    assert(new Timestamp((s.asReadOnlyByteBuffer().getLong)).toString == d1)
  }

  test("testSerializeString") {
    import java.nio.charset.Charset
    val s                = serialize("test_database_811656")
    val buffer           = new ByteArrayOutputStream()
    val dataOutputStream = new DataOutputStream(buffer)
    dataOutputStream.write("test_database_811656".getBytes(Charset.forName("utf8")))
    dataOutputStream.flush()

    val d1 = deserialize[String](TypeId.StringType, buffer.toByteArray)

    assert(s"${new String(s.toByteArray, Charset.forName("utf8"))}" == d1)
  }

  test("testSerializeIntCanBeString") {
    val s = serialize(100)

    val buffer           = new ByteArrayOutputStream()
    val dataOutputStream = new DataOutputStream(buffer)
    dataOutputStream.writeInt(100)
    dataOutputStream.flush()

    val d1 = deserialize[String](TypeId.IntType, buffer.toByteArray)

    assert(s.asReadOnlyByteBuffer().getInt == d1.toInt)
  }

  test("testSerializeLongCanBeDate") {
    val date = new Date(System.currentTimeMillis())
    val s    = serialize(date)

    val buffer           = new ByteArrayOutputStream()
    val dataOutputStream = new DataOutputStream(buffer)
    dataOutputStream.writeLong(date.getTime)
    dataOutputStream.flush()

    val d1 = deserialize[Date](TypeId.DateType, buffer.toByteArray)

    assert(s.asReadOnlyByteBuffer().getLong == d1.getTime)
  }

  test("testSerializeBooleanCanBeInt") {
    val s                = serialize(true)
    val buffer           = new ByteArrayOutputStream()
    val dataOutputStream = new DataOutputStream(buffer)
    dataOutputStream.writeBoolean(true)
    dataOutputStream.flush()

    val d1 = deserialize[Boolean](TypeId.BooleanType, buffer.toByteArray)

    assert(s.asReadOnlyByteBuffer().get() == 1 && d1)
  }

  test("testSerializeBooleanCanBeString") {
    val s                = serialize(true)
    val buffer           = new ByteArrayOutputStream()
    val dataOutputStream = new DataOutputStream(buffer)
    dataOutputStream.writeBoolean(true)
    dataOutputStream.flush()

    val d1 = deserialize[String](TypeId.BooleanType, buffer.toByteArray)

    assert(s.asReadOnlyByteBuffer().get() == 1 && d1 == "true")
  }

  test("testSerializeShortCanBeString") {
    val s                = serialize(100.toShort)
    val buffer           = new ByteArrayOutputStream()
    val dataOutputStream = new DataOutputStream(buffer)
    dataOutputStream.writeShort(100)
    dataOutputStream.flush()

    val d1 = deserialize[String](TypeId.ShortType, buffer.toByteArray)

    assert(s.asReadOnlyByteBuffer().getShort() == d1.toShort)
  }
