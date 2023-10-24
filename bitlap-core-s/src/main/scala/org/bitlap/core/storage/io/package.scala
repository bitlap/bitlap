/**
 * Copyright (C) 2023 bitlap.org .
 */
package org.bitlap.core.storage.io

import java.nio.ByteBuffer
import java.util
import java.util.Arrays

import org.apache.avro.generic.GenericRecord
import org.apache.avro.util.Utf8
import org.apache.parquet.filter2.compat.FilterCompat
import org.apache.parquet.filter2.predicate.FilterPredicate

extension (predicate: FilterPredicate) {

  def compact(): FilterCompat.Filter = {
    if (predicate == null) return FilterCompat.NOOP
    FilterCompat.get(predicate)
  }
}

extension (record: GenericRecord) {

  /** get default value [default] if the [key] does not exist.
   */
  def getWithDefault[T](key: String, default: T): T = {
    if (record == null || !record.hasField(key)) return default

    val value = record.get(key)
    (value match {
      case v: Utf8 => v.toString
      case v: ByteBuffer if v.hasArray =>
        util.Arrays.copyOfRange(v.array(), v.position(), v.limit())

      case v: ByteBuffer =>
        val oldPosition = v.position()
        v.position(0)
        val size    = v.limit()
        val buffers = new Array[Byte](size)
        v.get(buffers)
        v.position(oldPosition)
        buffers

      case _ => record.get(key)
    }).asInstanceOf[T]
  }
}
