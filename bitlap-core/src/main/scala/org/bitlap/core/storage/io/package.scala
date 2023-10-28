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
    if (predicate == null) FilterCompat.NOOP
    else FilterCompat.get(predicate)
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
